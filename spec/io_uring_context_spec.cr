require "./spec_helper"

private def nursery
  NestedScheduler::ThreadPool.nursery(1, io_context: NestedScheduler::IoUringContext.new, name: "uring") do |pl|
    yield pl
  end
end

describe NestedScheduler::IoUringContext do
  pending "HANDLES MULTIPLE WORKING THREADS (NOT)"

  it "works with enclosing scope" do
    run = false
    nursery &.spawn { run = true }
    run.should be_true
  end

  describe "#wait_readable" do
    it "yields when file becomes readable" do
      str = "hello world!"
      left, right = UNIXSocket.pair
      readable = false
      nursery do |pl|
        pl.spawn do
          sleep 0.001
          right.write(str.to_slice)
        end
        pl.spawn do
          left.wait_readable
          readable = true
        end
        pl.spawn do
          sleep 0.0005
          readable.should eq false
          sleep 0.0011
          readable.should eq true
        end
      end
    end

    it "supports timeouts" do
      str = "hello world!"
      left, right = UNIXSocket.pair
      has_timed_out = false
      nursery do |pl|
        pl.spawn do
          left.wait_readable(0.001.seconds) do
            has_timed_out = true
          end
        end
        pl.spawn do
          sleep 0.0005
          has_timed_out.should eq false
          sleep 0.0015
          has_timed_out.should eq true
        end
      end
    end
  end

  describe "write" do
    it "Can write to stdout" do
      # nice for error printing ..
      # kernel 5.8 is not enough for this. 5.11 is, so it was fixed at some point.
      nursery &.spawn { puts }
    end

    it "write" do
      filename = "test/write1"
      nursery &.spawn { File.write filename, "hello world" }
      File.read("test/write1").should eq "hello world"
    end
  end

  it "works with channels" do
    done = Channel(Nil).new(1)
    nursery &.spawn { done.send nil }
    done.receive.should be_nil

    done2 = Channel(Nil).new
    nursery do |pl|
      pl.spawn { done2.send nil }
      pl.spawn { done2.receive.should be_nil }
    end
  end

  it "#sleep" do
    sleep_time = 0.1
    spent_time = Time.measure do
      nursery do |pl|
        5.times do |i|
          pl.spawn do
            sleep sleep_time
          end
        end
      end
    end.to_f

    spent_time.should be > sleep_time
    spent_time.should be < 5 * sleep_time
  end

  describe "#accept" do
    it "can accept" do
      port = unused_local_port
      server = Socket.new(Socket::Family::INET, Socket::Type::STREAM, Socket::Protocol::TCP)
      server.bind("0.0.0.0", port)
      server.listen

      spawn { TCPSocket.new("127.0.0.1", port).close }

      client = nil
      nursery &.spawn { client = server.accept }

      # expectations outside spawn block just to be sure it runs.
      client.not_nil!.family.should eq(Socket::Family::INET)
      client.not_nil!.type.should eq(Socket::Type::STREAM)
      client.not_nil!.protocol.should eq(Socket::Protocol::TCP)

      client.not_nil!.close
      server.close
    end

    it "can wait for acceptance" do
      port = unused_local_port
      server = Socket.new(Socket::Family::INET, Socket::Type::STREAM, Socket::Protocol::TCP)
      server.bind("127.0.0.1", port)
      server.listen
      client = nil
      nursery do |n|
        n.spawn { sleep 0.001; TCPSocket.new("127.0.0.1", port).close }
        n.spawn { client = server.accept }
      end

      # expectations outside spawn block just to be sure it runs.
      client.not_nil!.family.should eq(Socket::Family::INET)
      client.not_nil!.type.should eq(Socket::Type::STREAM)
      client.not_nil!.protocol.should eq(Socket::Protocol::TCP)

      client.not_nil!.close
      server.close
    end

    it "accept raises timeout error if read_timeout is specified" do
      server = Socket.new(Socket::Family::INET, Socket::Type::STREAM, Socket::Protocol::TCP)
      port = unused_local_port
      server.bind("0.0.0.0", port)
      server.read_timeout = 0.1
      nursery &.spawn { server.listen }

      expect_raises(IO::TimeoutError) { server.accept }
      expect_raises(IO::TimeoutError) { server.accept? }
    end
  end

  describe "#connect" do
    it "can connect" do
      port = unused_local_port
      server = Socket.new(Socket::Family::INET, Socket::Type::STREAM, Socket::Protocol::TCP)
      server.bind("127.0.0.1", port)
      server.listen
      client = nil

      nursery do |n|
        n.spawn { sleep 0.001; TCPSocket.new("127.0.0.1", port).close }
      end

      client = server.accept
      client.not_nil!.family.should eq(Socket::Family::INET)
      client.not_nil!.type.should eq(Socket::Type::STREAM)
      client.not_nil!.protocol.should eq(Socket::Protocol::TCP)

      client.not_nil!.close
      server.close
    end

    it "tests read_timeout" do
      nursery do |n|
        n.spawn do
          test_server("localhost", 0, 0) do |server|
            client = HTTP::Client.new("localhost", server.local_address.port)
            client.read_timeout = 0.1.seconds
            client.get("/")
          end

          # Here we don't want to write a response on the server side because
          # it doesn't make sense to try to write because the client will already
          # timeout on read. Writing a response could lead on an exception in
          # the server if the socket is closed.
          test_server("localhost", 0, 0.05, write_response: false) do |server|
            client = HTTP::Client.new("localhost", server.local_address.port)
            expect_raises(IO::TimeoutError, "Read timed out") do
              client.read_timeout = 0.0001
              client.get("/?sleep=1")
            end
          end
        end
      end
    end

    it "tests write_timeout", focus: true do
      nursery do |n|
        n.spawn do
          # Here we don't want to write a response on the server side because
          # it doesn't make sense to try to write because the client will already
          # timeout on read. Writing a response could lead on an exception in
          # the server if the socket is closed.
          test_server("localhost", 0, 0, write_response: false) do |server|
            client = HTTP::Client.new("localhost", server.local_address.port)
            expect_raises(IO::TimeoutError, "Write timed out") do
              client.write_timeout = 0.001
              client.post("/", body: "a" * 5_000_000)
              p :e
            end
          end
    #    rescue e
#          e.inspect_with_backtrace(STDERR)
          #   Crystal::System.print_error "exc: #{e.inspect}\n"
        end
      end
    end

    it "tests connect_timeout" do
      test_server("localhost", 0, 0) do |server|
        client = HTTP::Client.new("localhost", server.local_address.port)
        client.connect_timeout = 0.5
        client.get("/")
      end
    end
  end

  it "sends messages" do
    port = unused_local_port
    server = Socket.tcp(Socket::Family::INET6)
    server.bind("::1", port)
    server.listen
    address = Socket::IPAddress.new("::1", port)
    socket = Socket.tcp(Socket::Family::INET6)
    socket.connect(address)
    client = server.not_nil!.accept

    nursery do |pl|
      pl.spawn do
        client.gets.should eq "foo"
        client.puts "bar"
      end
      pl.spawn do
        socket.puts "foo"
        socket.gets.should eq "bar"
      end
    end
  ensure
    client.try &.close
    socket.try &.close
    server.try &.close
  end

  pending "socket timeouts"

  each_ip_family do |family, address, unspecified_address|
    it "sends and receives messages" do
      port = unused_local_port

      server = UDPSocket.new(family)
      server.bind(address, port)
      server.local_address.should eq(Socket::IPAddress.new(address, port))

      client = UDPSocket.new(family)
      client.bind(address, 0)

      nursery do |pl|
        pl.spawn { client.send "message", to: server.local_address }
        pl.spawn { server.receive.should eq({"message", client.local_address}) }
      end

      client.connect(address, port)
      client.local_address.family.should eq(family)
      client.local_address.address.should eq(address)
      client.remote_address.should eq(Socket::IPAddress.new(address, port))

      nursery do |pl|
        pl.spawn { client.send "message" }
        pl.spawn { server.receive.should eq({"message", client.local_address}) }
      end

      buffer = uninitialized UInt8[256]

      nursery do |pl|
        pl.spawn { client.send("laus deo semper") }
        pl.spawn do
          bytes_read, client_addr = server.receive(buffer.to_slice)
          message = String.new(buffer.to_slice[0, bytes_read])
          message.should eq("laus deo semper")
        end
      end

      nursery do |pl|
        pl.spawn { client.send("laus deo semper") }
        pl.spawn do
          bytes_read, client_addr = server.receive(buffer.to_slice[0, 4])
          message = String.new(buffer.to_slice[0, bytes_read])
          message.should eq("laus")
        end
      end

      client.close
      server.close
    end
  end
end

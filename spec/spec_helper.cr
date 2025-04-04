require "spec"
require "../src/nested_scheduler_io_uring_context"
require "http/client"

def unused_local_port
  TCPServer.open("::", 0) do |server|
    server.local_address.port
  end
end

# from http client spec.
def test_server(host, port, read_time = 0, content_type = "text/plain", write_response = true)
  server = TCPServer.new(host, port)
  begin
    spawn do
      io = server.accept
      sleep read_time
      if write_response
        response = HTTP::Client::Response.new(200, headers: HTTP::Headers{"Content-Type" => content_type}, body: "OK")
        response.to_io(io)
        io.flush
      end
    end

    yield server
  ensure
    server.close
  end
end

def each_ip_family(&block : Socket::Family, String, String ->)
  describe "using IPv4" do
    block.call Socket::Family::INET, "127.0.0.1", "0.0.0.0"
  end

  describe "using IPv6" do
    block.call Socket::Family::INET6, "::1", "::"
  end
end

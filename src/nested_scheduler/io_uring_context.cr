require "nested_scheduler"
require "ior"
require "../monkeypatch/fiber"

module NestedScheduler
  class IoUringContext < IOContext
    # What is a good waittime? Perhaps it needs to be a backoff?
    # TODO: make use of ring timeout instead for this.
    WAIT_TIMESPEC = LibC::Timespec.new(tv_sec: 0, tv_nsec: 50_000)

    getter :ring

    #    getter :scheduler ::Crystal::Scheduler

    def initialize(context = nil, size = 32)
      @ring = IOR::IOUring.new size: size
      # Set up a timeout with userdata 0. There will always be one,
      # and only one of these in flight. The purpose is to allow
      # preemption of other stuff. This also has the upside that we
      # can *always* do a blocking wait. No reason to actually submit
      # it until we may want to wait though.
      get_sqe.timeout(pointerof(WAIT_TIMESPEC), user_data: 0)
    end

    def new : self
      self.class.new(self)
    end

    def wait_readable(io, scheduler, timeout)
      # TODO: Actually do timeouts.
      get_sqe.poll_add(io, :POLLIN, user_data: userdata(scheduler))
      ring_wait(scheduler) do |cqe|
        yield if cqe.canceled?

        raise ::IO::Error.from_os_error("poll", cqe.errno) unless cqe.success?
      end
    end

    def wait_writable(io, scheduler, timeout)
      # TODO: Actually do timeouts..
      get_sqe.poll_add(io, :POLLOUT, user_data: userdata(scheduler))
      ring_wait(scheduler) do |cqe|
        yield if cqe.canceled?

        raise ::IO::Error.from_os_error("poll", cqe.errno) unless cqe.success?
      end
    end

    def accept(socket, scheduler, timeout)
      # TODO: Timeout..
      loop do
        get_sqe.accept(socket, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          if cqe.success?
            return cqe.to_i
          elsif socket.closed?
            return nil
          elsif cqe.eagain? # must be only non-escaping branch
          else
            raise ::IO::Error.from_os_error("accept", cqe.errno)
          end
        end
        # # Nonblocking sockets return EAGAIN if there isn't an
        # # active connection attempt. To detect that wait_readable
        # # is needed but that needs to happen outside ring_wait due
        # # to the cqe needs to be marked as seen.
        wait_readable(socket, scheduler, timeout) do
          raise Socket::TimeoutError.new("Accept timed out")
        end
      end
    end

    def connect(socket, scheduler, addr, timeout)
      loop do
        get_sqe.connect(socket, addr.to_unsafe.address, addr.size,
          user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe.errno
          when Errno::NONE, Errno::EISCONN
            return
          when Errno::EINPROGRESS, Errno::EALREADY
          else
            return yield Socket::ConnectError.from_os_error("connect", os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, timeout: timeout) do
          return yield ::IO::TimeoutError.new("connect timed out")
        end
      end
    end

    def send(socket, scheduler, slice : Bytes, errno_message : String) : Int32
      loop do
        get_sqe.send(socket, slice, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success?
            return cqe.to_i
          when .eagain?
          else
            raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, socket.write_timeout) do
          raise ::IO::TimeoutError.new("connect timed out")
        end
      end
    end

    def send_to(socket, scheduler, message, to addr : Socket::Address) : Int32
      slice = message.to_slice

      # No sendto in uring, falling back to sendmsg.
      vec = LibC::IOVec.new(base: slice.to_unsafe, len: slice.size)
      hdr = LibC::MsgHeader.new(
        name: addr.to_unsafe.as(LibC::SockaddrStorage*),
        namelen: LibC::SocklenT.new(sizeof(LibC::SockaddrStorage)),
        iov: pointerof(vec),
        iovlen: 1
      )

      get_sqe.sendmsg(socket, pointerof(hdr), user_data: userdata(scheduler))
      ring_wait(scheduler) do |cqe|
        if cqe.success?
          cqe.to_i.to_i32
        else
          raise ::IO::Error.from_os_error("Error sending datagram to #{addr}", os_error: cqe.errno)
        end
      end
    end

    # TODO: handle write timeout, errmess
    def socket_write(socket, scheduler, slice : Bytes, errno_message : String) : Nil
      loop do
        get_sqe.send(socket, slice, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success?
            bytes_written = cqe.to_i
            slice += bytes_written
            return if slice.size == 0
          when .eagain?
          else raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, timeout: socket.write_timeout) do
          raise ::IO::TimeoutError.new("socket write timed out")
        end
      end
    end

    # TODO: handle read timeout
    def recv(socket, scheduler, slice : Bytes, errno_message : String)
      loop do
        get_sqe.recv(socket, slice, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success? then return cqe.to_i
          when .eagain?
          else raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_readable(socket, scheduler, timeout: socket.read_timeout) do
          raise ::IO::TimeoutError.new("recv timed out")
        end
      end
    end

    # todo timeout.., errmess
    def recvfrom(socket, scheduler, slice, sockaddr, addrlen, errno_message : String)
      # No recvfrom in uring, falling back to recvmsg.
      vec = LibC::IOVec.new(base: slice.to_unsafe, len: slice.size)
      hdr = LibC::MsgHeader.new(
        name: sockaddr.as(LibC::SockaddrStorage*),
        namelen: addrlen,
        iov: pointerof(vec),
        iovlen: 1
      )
      # Fixme errono
      loop do
        get_sqe.recvmsg(socket, pointerof(hdr), user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success? then return cqe.to_i
          when .eagain?
          else raise ::IO::Error.from_os_error(message: errno_message, os_error: cqe.errno)
          end
        end
        wait_readable(socket, scheduler, timeout: socket.read_timeout) do
          raise ::IO::TimeoutError.new("receive timed out")
        end
      end
    end

    # TODO: handle read timeout
    def read(io, scheduler, slice : Bytes)
      # Loop due to EAGAIN. EAGAIN happens at least once during
      # scheduler setup. I'm not totally happy with doing read in a
      # loop like this but I havn't figured out a better way to make
      # it work.
      loop do
        get_sqe.read(io, slice, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success? then return cqe.to_i
          when .eagain?
          when .bad_file_descriptor? then raise ::IO::Error.from_os_error(message: "File not open for reading", os_error: cqe.errno)
          else                            raise ::IO::Error.from_os_error(message: "Read Error", os_error: cqe.errno)
          end
        end
        wait_readable(io, scheduler, timeout: io.read_timeout) do
          raise ::IO::TimeoutError.new("read timed out")
        end
      end
    end

    # TODO: add write timeout
    def write(io, scheduler, slice : Bytes)
      loop do
        get_sqe.write(io, slice, user_data: userdata(scheduler))
        ring_wait(scheduler) do |cqe|
          case cqe
          when .success? then return cqe.to_i
          when .eagain?
          when .bad_file_descriptor? then raise ::IO::Error.from_os_error(message: "File not open for writing", os_error: cqe.errno)
          else                            raise ::IO::Error.from_os_error(message: "Write error", os_error: cqe.errno)
          end
        end
        wait_writable(io, scheduler, timeout: io.write_timeout) do
          raise ::IO::TimeoutError.new("recvfrom timed out")
        end
      end
    end

    def sleep(scheduler, fiber, time) : Nil
      ts = LibC::Timespec.new(tv_sec: 0, tv_nsec: 50_000)

      timespec = LibC::Timespec.new(
        tv_sec: LibC::TimeT.new(time.to_i),
        tv_nsec: time.nanoseconds
      )
      get_sqe.timeout(pointerof(timespec), user_data: userdata(fiber))
      ring_wait(scheduler) { }
    end

    def yield(scheduler, fiber)
      get_sqe.nop(user_data: userdata(fiber))
      ring_wait(scheduler) { }
    end

    def yield(fiber, to other)
      get_sqe.nop(user_data: userdata(fiber))
      # Normally reschedule submits but here the scheduler resumes
      # explicitly.
      ring.submit
    end

    def prepare_close(_file)
      # Do we need to cancel pending events on the file?
      # EDIT: Yes, especially as the file can be used in both libevent
      # and uring.
    end

    def close(fd, scheduler)
      get_sqe.close(fd, user_data: userdata(scheduler))
      ring_wait(scheduler) do |cqe|
        return if cqe.success?
        return if cqe.errno.eintr? || cqe.errno.einprogress?

        raise ::IO::Error.from_os_error("Error closing file", cqe.errno)
      end
    end

    # TODO: handle submit failure?
    def reschedule(scheduler)
      # Controls the ring submit as the submit_and_wait variant saves
      # us a syscall.
      loop do
        if runnable = yield
          # Can't do linked timeouts if there are not at least 2 sqe
          # slots left.
          ring.submit if ring.unsubmitted? && (ring.sq_space_left < 2)
        else
          # Note that #wait actually don't do a syscall after
          # #submit_and_wait as there is a waiting cqe already.
          ring.submit_and_wait if ring.unsubmitted?
          cqe = ring.wait

          next if handle_autowakeup?(cqe)
          runnable = process_cqe(cqe)
          while cqe = ring.peek
            next if handle_autowakeup?(cqe)

            scheduler.actually_enqueue(process_cqe(cqe))
          end
        end
        runnable.resume unless runnable == Fiber.current
        break
      end
    end

    @[AlwaysInline]
    private def process_cqe(cqe) : Fiber
      if cqe.ring_error?
        Crystal::System.print_error "BUG: IO URing error: #{cqe.error_message}\n"
        exit
      end

      fiber = Pointer(Fiber).new(cqe.user_data).as(Fiber)
      fiber.completion_result = cqe.result
      ring.seen cqe
      fiber
    end

    @[AlwaysInline]
    private def handle_autowakeup?(cqe)
      if cqe.user_data.zero?
        # That is, CQE is timeout that has expired. Read the
        # timeout and try another iteration and see if anything
        # can be done now.

        # TODO: Instead of recurring timeouts like this, make use
        # of the new timeouts on submit_and_wait
        ring.seen cqe
        get_sqe.timeout(pointerof(WAIT_TIMESPEC), user_data: 0)
        true
      else
        false
      end
    end

    @[AlwaysInline]
    private def get_sqe
      if sqe = ring.sqe
        sqe
      else
        # TODO: handle error
        ring.submit
        ring.sqe.not_nil!
      end
    end

    private def ring_wait(scheduler : Crystal::Scheduler)
      scheduler.actually_reschedule

      fiber = scheduler.@current
      yield fiber.completion_result
    end

    @[AlwaysInline]
    private def userdata(scheduler : Crystal::Scheduler)
      scheduler.@current.object_id
    end

    @[AlwaysInline]
    private def userdata(fiber : Fiber)
      fiber.object_id
    end
  end
end

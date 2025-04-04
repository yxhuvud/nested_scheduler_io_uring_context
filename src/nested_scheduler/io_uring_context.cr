require "nested_scheduler"
require "ior"
require "../monkeypatch/fiber"

module NestedScheduler
  class IoUringContext < IOContext
    # What is a good waittime? Perhaps it needs to be a backoff?
    # TODO: Make use of notifier instead of timeouts.
    WAIT_TIMESPEC = LibC::Timespec.new(tv_sec: 0, tv_nsec: 50_000)

    getter :ring

    def initialize(context = nil, size = 64)
      @ring = IOR::IOUring.new size: size
    end

    def new : self
      self.class.new(self)
    end

    def wait_readable(io, scheduler, timeout)
      get_sqe.poll_add(io, LibC::POLL_FLAG::POLLIN | LibC::POLL_FLAG::POLLEXCLUSIVE,
        user_data: userdata(scheduler), io_link: timeout)
      link_timeout(timeout)
      ring_wait(scheduler) do |cqe|
        # Strictly speaking we don't know if the op was canceled due
        # to timeout or something else. Perhaps there is need to
        # handle that, but hopefully not.
        return yield if cqe.canceled?

        raise ::IO::Error.from_os_error("poll", cqe.errno) unless cqe.success?
      end
    end

    def wait_writable(io, scheduler, timeout)
      get_sqe.poll_add(io, LibC::POLL_FLAG::POLLOUT | LibC::POLL_FLAG::POLLEXCLUSIVE,
        user_data: userdata(scheduler), io_link: timeout)
      link_timeout(timeout)
      ring_wait(scheduler) do |cqe|
        return yield if cqe.canceled?

        raise ::IO::Error.from_os_error("poll", cqe.errno) unless cqe.success?
      end
    end

    def accept(socket, scheduler, timeout)
      loop do
        get_sqe.accept(socket, user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?      then return cqe.to_i
          when .canceled?     then raise Socket::TimeoutError.new("Accept timed out")
          when socket.closed? then return nil
          else                     raise ::IO::Error.from_os_error("accept", cqe.errno)
          end
        end
        # Nonblocking sockets return EAGAIN if there isn't an
        # active connection attempt. To detect that wait_readable
        # is needed but that needs to happen outside ring_wait due
        # to the cqe needs to be marked as seen.
        wait_readable(socket, scheduler, timeout) do
          raise Socket::TimeoutError.new("Accept timed out")
        end
      end
    end

    def connect(socket, scheduler, addr, timeout)
      loop do
        get_sqe.connect(socket, addr.to_unsafe.address, addr.size,
          user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe.errno
          when Errno::EINPROGRESS, Errno::EALREADY
          when Errno::NONE, Errno::EISCONN then return
          when Errno::ECANCELED            then return yield ::IO::TimeoutError.new("connect timed out")
          else                                  return yield Socket::ConnectError.from_os_error("connect", os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, timeout: timeout) do
          return yield ::IO::TimeoutError.new("connect timed out")
        end
      end
    end

    def send(socket, scheduler, slice : Bytes, errno_message : String, timeout = socket.write_timeout) : Int32
      loop do
        get_sqe.send(socket, slice, user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?  then return cqe.to_i
          when .canceled? then raise ::IO::TimeoutError.new("Send timed out")
          else                 raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, timeout) do
          raise ::IO::TimeoutError.new("Send timed out")
        end
      end
    end

    def send_to(socket, scheduler, message, to addr : Socket::Address) : Int32
      slice = message.to_slice

      # No sendto in uring, falling back to sendmsg.
      # TODO: Use send, it is being expanded to cover sendto in 5.17-18?
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

    def socket_write(socket, scheduler, slice : Bytes, errno_message : String, timeout = socket.write_timeout) : Nil
      loop do
        get_sqe.send(socket, slice, user_data: userdata(scheduler) , io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?
            bytes_written = cqe.to_i
            slice += bytes_written
            return if slice.size == 0
          when .canceled? then raise ::IO::TimeoutError.new("socket write timed out")
          else                 raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_writable(socket, scheduler, timeout: timeout) do
          raise ::IO::TimeoutError.new("socket write timed out")
        end
      end
    end

    def recv(socket, scheduler, slice : Bytes, errno_message : String, timeout = socket.read_timeout)
      loop do
        get_sqe.recv(socket, slice, user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?  then return cqe.to_i
          when .canceled? then raise ::IO::TimeoutError.new("Recv timed out")
          else                 raise ::IO::Error.from_os_error(errno_message, os_error: cqe.errno)
          end
        end
        wait_readable(socket, scheduler, timeout: timeout) do
          raise ::IO::TimeoutError.new("Recv timed out")
        end
      end
    end

    # todo timeout.., errmess
    def recvfrom(socket, scheduler, slice, sockaddr, addrlen, errno_message : String, timeout = socket.read_timeout)
      # No recvfrom in uring, falling back to recvmsg.
      vec = LibC::IOVec.new(base: slice.to_unsafe, len: slice.size)
      hdr = LibC::MsgHeader.new(
        name: sockaddr.as(LibC::SockaddrStorage*),
        namelen: addrlen,
        iov: pointerof(vec),
        iovlen: 1
      )

      loop do
        get_sqe.recvmsg(socket, pointerof(hdr), user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?  then return cqe.to_i
          when .canceled? then raise ::IO::TimeoutError.new("receive timed out")
          else                 raise ::IO::Error.from_os_error(message: errno_message, os_error: cqe.errno)
          end
        end
        wait_readable(socket, scheduler, timeout: timeout) do
          raise ::IO::TimeoutError.new("receive timed out")
        end
      end
    end

    def read(io, scheduler, slice : Bytes, timeout = io.read_timeout)
      loop do
        get_sqe.read(io, slice, user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
#        Crystal::System.print_error "#{io.inspect}\n"
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
  #          Crystal::System.print_error "eag\n"
          when .success?             then return cqe.to_i
          when .canceled?            then raise ::IO::TimeoutError.new("read timed out")
          when .bad_file_descriptor? then raise ::IO::Error.from_os_error(message: "File not open for reading", os_error: cqe.errno)
          else                            raise ::IO::Error.from_os_error(message: "Read Error", os_error: cqe.errno)
          end
        end
 #       Crystal::System.print_error "WR\n"
        wait_readable(io, scheduler, timeout: timeout) do
          raise ::IO::TimeoutError.new("read timed out")
        end
      end
    end

    # TODO: add write timeout
    def write(io, scheduler, slice : Bytes, timeout = io.write_timeout)
      loop do
        get_sqe.write(io, slice, user_data: userdata(scheduler), io_link: timeout)
        link_timeout(timeout)
        ring_wait(scheduler) do |cqe|
          case cqe
          when .eagain?
          when .success?             then return cqe.to_i
          when .canceled?            then raise ::IO::TimeoutError.new("write timed out")
          when .bad_file_descriptor? then raise ::IO::Error.from_os_error(message: "File not open for writing", os_error: cqe.errno)
          else                            raise ::IO::Error.from_os_error(message: "Write error", os_error: cqe.errno)
          end
        end
        wait_writable(io, scheduler, timeout: timeout) do
          raise ::IO::TimeoutError.new("write timed out")
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
      loop do
        if runnable = yield
          # TODO: Is there a need for a deadline where events are
          # always submitted if too old? Compute heavy load with few
          # events might get higher latency than what is nice? Or
          # should submit simply be eager?

          # Can't do linked timeouts if there are not at least 2 sqe
          # slots left, so leave some space for that.
          ring.submit if ring.unsubmitted? # && (ring.sq_space_left < 2)
        else
          # TODO: Change to submit_and_wait with timeout when that is
          # supported.
          ring.submit if ring.unsubmitted?
          cqe = ring.wait(timeout: pointerof(WAIT_TIMESPEC))
          # Wait timeout has occurred, check if anything has been
          # unscheduled in the interim.
          # TODO: Change to notifier on enqueue?
          next unless cqe
          next if skippable?(cqe)

          runnable = process_cqe(cqe)

          # Tried to get peek(into: ) to do the same, more
          # efficiently, but got some strange error that I don't
          # understand.
          while cqe = ring.peek
#            Crystal::System.print_error "ab\n"
            next if skippable?(cqe)
            scheduler.actually_enqueue(process_cqe(cqe))
          end
        end
        runnable.resume unless runnable == Fiber.current
        break
      end
    end

    @[AlwaysInline]
    private def skippable?(cqe)
 #      Crystal::System.print_error "skip\n"
      # IO timeout has occurred. Do nothing, the result is handled
      # in CQE corresponding to the linked SQE.
      if cqe.user_data == 0
        ring.seen cqe
        true
      else
        false
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

    # Needs to be a macro to keep the timespec in scope
    macro link_timeout(timeout)
      if %timeout = {{timeout}}
        %timespec = LibC::Timespec.new(tv_sec: %timeout.to_i, tv_nsec: %timeout.nanoseconds)
        get_sqe.link_timeout(pointerof(%timespec), user_data: 0)
      end
    end
  end
end

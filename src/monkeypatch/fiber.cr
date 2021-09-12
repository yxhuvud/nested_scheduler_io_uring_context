require "fiber"

class Fiber
  # FIXME: This will probably need an implementation for wait_readable
  # etc as the builtin polling will wake all waiting fibers at once.
  def resume_event
    if p = Thread.current.scheduler.pool
      if p.io_context.class == NestedScheduler::IoUringContext
        Crystal::System.print_error "This is not implemented yet, sorry.\n"
        exit
      end
    end
    @resume_event ||= Crystal::EventLoop.create_resume_event(self)
  end

  def timeout_event
    if p = Thread.current.scheduler.pool
      if p.io_context.class == NestedScheduler::IoUringContext
        Crystal::System.print_error "BUG: This should not be reachable.\n"
        exit
      end
    end
    @timeout_event ||= Crystal::EventLoop.create_timeout_event(self)
  end
end

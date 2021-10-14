require "fiber"
require "ior"

class Fiber
  @completion_result : IOR::CQE::Result?

  def completion_result=(result : IOR::CQE::Result?)
    @completion_result = result
  end

  def completion_result : IOR::CQE::Result
    result = @completion_result
    @completion_result = nil
    result ? result : raise "BUG: Missing completion for fiber #{inspect}"
  end

  # # FIXME: This will probably need an implementation for wait_readable
  # # etc as the builtin polling will wake all waiting fibers at once.
  # def resume_event
  #   if p = Thread.current.scheduler.pool
  #     if p.io_context.class == NestedScheduler::IoUringContext
  #       Crystal::System.print_error "This is not implemented yet, sorry.\n"
  #       exit
  #     end
  #   end
  #   @resume_event ||= Crystal::EventLoop.create_resume_event(self)
  # end

  # def timeout_event
  #     if p = Thread.current.scheduler.pool
  #       if p.io_context.class == NestedScheduler::IoUringContext
  #         Crystal::System.print_error "BUG: This should not be reachable.\n"
  #         exit
  #       end
  #     end
  #     @timeout_event ||= Crystal::EventLoop.create_timeout_event(self)
  # end
end

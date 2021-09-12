# nested_scheduler_io_uring_context
Alternative event loop for nested_scheduler that make use of io_uring
instead of libevent.

## Installation
0. Have Linux Kernel 5.11+ installed.
1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     nested_scheduler_io_uring_context:
       github: yxhuvud/nested_scheduler_io_uring_context
   ```

2. Run `shards install`

## Usage

```crystal
require "nested_scheduler_io_uring_context"
```

TODO: Write usage instructions here

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/your-github-user/nested_scheduler_io_uring_context/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Linus Sellberg](https://github.com/yxhuvud) - creator and maintainer

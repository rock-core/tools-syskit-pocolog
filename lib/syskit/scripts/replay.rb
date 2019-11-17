require 'syskit/log/cli/log'

class CLI < Thor
    desc 'replay', 'replaying of log data'
    subcommand 'replay', Syskit::Log::CLI::Replay
end

Roby.display_exception do
    CLI.start(['replay', *ARGV])
end


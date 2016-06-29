require 'syskit/pocolog/cli/pocolog'
class CLI < Thor
    desc 'pocolog', 'replaying of pocolog data into a Syskit network'
    subcommand 'pocolog', Syskit::Pocolog::CLI::Pocolog
end

Roby.display_exception do
    CLI.start(['pocolog', *ARGV])
end


require 'syskit/log/cli/datastore'

class CLI < Thor
    desc 'datastore', 'data management'
    subcommand 'datastore', Syskit::Log::CLI::Datastore
end

Roby.display_exception do
    CLI.start(['datastore', *ARGV])
end

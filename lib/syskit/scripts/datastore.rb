require 'syskit/pocolog/cli/datastore'

class CLI < Thor
    desc 'datastore', 'data management'
    subcommand 'datastore', Syskit::Pocolog::CLI::Datastore
end

Roby.display_exception do
    CLI.start(['datastore', *ARGV])
end


require 'syskit/pocolog/cli'

Roby.display_exception do
    Syskit::Pocolog::CLI.start(ARGV)
end


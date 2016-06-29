require "pocolog"
require "syskit"

module Syskit::Pocolog
    extend Logger::Root('Syskit::Pocolog', Logger::WARN)
end

require 'digest/sha2'
require "metaruby/dsls/find_through_method_missing"
require 'pocolog/cli/null_reporter'
require "syskit/pocolog/version"
require "syskit/pocolog/exceptions"
require "syskit/pocolog/lazy_data_stream"
require "syskit/pocolog/streams"
require "syskit/pocolog/task_streams"
require "syskit/pocolog/rock_stream_matcher"

require "syskit/pocolog/models/deployment"
require "syskit/pocolog/deployment"
require "syskit/pocolog/models/replay_task_context"
require "syskit/pocolog/replay_task_context"
require "syskit/pocolog/replay_manager"

require 'syskit/pocolog/extensions'
require 'syskit/pocolog/shell_interface'
require 'syskit/pocolog/registration_namespace'
require 'syskit/pocolog/plugin'

require 'syskit/pocolog/datastore'

module Syskit::Pocolog
    # Returns the paths of the log files in a given directory
    #
    # The returned paths are sorted in 'pocolog' order, i.e. multi-IO files are
    # following each other in the order of their place in the overall IO
    # sequence
    #
    # @param [Pathname] dir_path path to the directory
    def self.logfiles_in_dir(dir_path)
        path = Pathname.new(dir_path).realpath

        paths = Array.new
        Pathname.glob(path + '*.*.log') do |path|
            basename = path.basename
            if basename.to_s =~ /(.*)\.(\d+)\.log$/
                paths << [$1, Integer($2), path]
            end
        end
        paths.sort.map { |_, _, path| path }
    end
end


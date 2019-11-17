require "pocolog"
require "syskit"

module Syskit
    module Log
        extend Logger::Root('Syskit::Log', Logger::WARN)
    end
end

require 'digest/sha2'
require "metaruby/dsls/find_through_method_missing"
require 'pocolog/cli/null_reporter'
require "syskit/log/version"
require "syskit/log/exceptions"
require "syskit/log/lazy_data_stream"
require "syskit/log/streams"
require "syskit/log/task_streams"
require "syskit/log/rock_stream_matcher"

require "syskit/log/models/deployment"
require "syskit/log/deployment"
require "syskit/log/models/replay_task_context"
require "syskit/log/replay_task_context"
require "syskit/log/replay_manager"

require 'syskit/log/extensions'
require 'syskit/log/shell_interface'
require 'syskit/log/registration_namespace'
require 'syskit/log/plugin'

require 'syskit/log/datastore'

module Syskit
    module Log
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
end

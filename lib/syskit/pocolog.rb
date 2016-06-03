require "pocolog"
require "syskit"

module Syskit::Pocolog
    extend Logger::Root('Syskit::Pocolog', Logger::WARN)
end

require "metaruby/dsls/find_through_method_missing"
require "syskit/pocolog/version"
require "syskit/pocolog/exceptions"
require "syskit/pocolog/streams"
require "syskit/pocolog/task_streams"
require "syskit/pocolog/rock_stream_matcher"
require "syskit/pocolog/configuration"

require "syskit/pocolog/models/deployment"
require "syskit/pocolog/deployment"
require "syskit/pocolog/models/replay_task_context"
require "syskit/pocolog/replay_task_context"
require "syskit/pocolog/execution_engine_extension"
require "syskit/pocolog/replay_manager"

require 'syskit/pocolog/shell_interface'

require 'syskit/pocolog/registration_namespace'

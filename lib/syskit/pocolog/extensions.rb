module Syskit::Pocolog
    # Set of modules used to extend Roby and Syskit's own classes
    module Extensions
        extend Logger::Hierarchy
        extend Logger::Forward
    end
end

require 'syskit/pocolog/extensions/deployment_group'
require 'syskit/pocolog/extensions/instance_requirements'
require 'syskit/pocolog/extensions/configuration'
require 'syskit/pocolog/extensions/execution_engine'

Syskit::Models::DeploymentGroup.class_eval do
    prepend Syskit::Pocolog::Extensions::DeploymentGroup
end
Syskit::InstanceRequirements.class_eval do
    prepend Syskit::Pocolog::Extensions::InstanceRequirements
end
Syskit::RobyApp::Configuration.class_eval do
    prepend Syskit::Pocolog::Extensions::Configuration
end
Roby::ExecutionEngine.class_eval do
    prepend Syskit::Pocolog::Extensions::ExecutionEngine
end


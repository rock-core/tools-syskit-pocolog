# frozen_string_literal: true

## One can require the configuration from another robot, for instance if one has
## a common robot class with minor modifications
#
# require 'config/robots/robot_class'

# Block evaluated at the very beginning of the Roby app initialization
Robot.init do
    ## Make models from another Roby app accessible
    # Relative paths are resolved from the root of this app
    # Roby.app.register_app('../separate_path')
end

# Block evaluated to load the models this robot requires
Robot.requires do
end

# Block evaluated to configure the system, that is set up values in Roby's Conf
# and State
Robot.config do
end

# Setup of the robot's main action interface
#
# Add use statements here, as e.g.
#
#   use_library Default::Actions::MyActionInterface
#
# or, if you're using syskit
#
#   use_profile Default::Profiles::BaseProfile
#
Robot.actions do
end

# Block evaluated when the Roby app is fully setup, and the robot ready to
# start. This is where one usually adds permanent tasks and/or status lines
Robot.controller do
end

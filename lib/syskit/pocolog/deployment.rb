module Syskit::Pocolog
    # Task supporting the replay process
    #
    # To give some control over how the streams are aligned together (and
    # potentially allow optimizing the replay process), streams supporting a
    # given task are injected in the replay process when they are associated
    # with a deployment task, and removed when the deployment is removed.
    class Deployment < Syskit::Deployment
        extend Models::Deployment
    end
end

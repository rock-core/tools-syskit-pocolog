module Syskit
    module Pocolog
        # Stream accessor for streams that have already be narrowed down to a
        # single task
        #
        # It is returned from the main stream pool by
        # {Streams#find_task_by_name)
        class TaskStreams < Streams
            # Find a port stream that matches the given name
            def find_port_by_name(name)
                objects = find_all_streams(RockStreamMatcher.new.ports.object_name(name))
                if objects.size > 1
                    raise Ambiguous, "there are multiple ports with the name #{name}"
                else objects.first
                end
            end

            # Find a property stream that matches the given name
            def find_property_by_name(name)
                objects = find_all_streams(RockStreamMatcher.new.properties.object_name(name))
                if objects.size > 1
                    raise Ambiguous, "there are multiple properties with the name #{name}"
                else objects.first
                end
            end

            # Syskit-looking accessors for ports (_port) and properties
            # (_property)
            def method_missing(m, *args)
                MetaRuby::DSLs.find_through_method_missing(
                    self, m, args,
                    "port" => "find_port_by_name",
                    "property" => "find_property_by_name") || super
            end
        end
    end
end


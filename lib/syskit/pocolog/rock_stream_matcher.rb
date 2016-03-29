module Syskit::Pocolog
    # A stream matcher for Rock's standard metadata
    #
    # It can be used as parameter to {Streams#query}
    #
    # The final query is a logical AND of all the characteristics
    class RockStreamMatcher
        attr_reader :query

        def initialize
            @query = Hash.new
        end

        def add_regex(key, rx)
            if existing = query[key]
                query[key] = Regexp.union(existing, rx)
            else
                query[key] =  rx
            end
            self
        end

        # Match only ports
        def ports
            add_regex('rock_stream_type', /^port$/)
        end

        # Match only properties
        def properties
            add_regex('rock_stream_type', /^property$/)
        end

        # Match the object (port/property) name
        def object_name(name)
            add_regex('rock_task_object_name', name)
        end

        # Match the task name
        def task_name(name)
            add_regex('rock_task_name', name)
        end

        # Match the task model
        def task_model(model)
            add_regex('rock_task_model', model.orogen_model.name)
        end

        # Tests whether a stream matches this query
        def ===(stream)
            query.all? do |key, matcher|
                if metadata = stream.metadata[key]
                    matcher === metadata
                end
            end
        end
    end
end


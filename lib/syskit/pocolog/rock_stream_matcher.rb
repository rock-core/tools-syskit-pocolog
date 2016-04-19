module Syskit::Pocolog
    # A stream matcher for Rock's standard metadata
    #
    # It can be used as parameter to {Streams#find_all_streams}
    #
    # The final query is a logical AND of all the characteristics
    class RockStreamMatcher
        # A mapping from a metadata key to a match object
        #
        # @return [Hash<String,#===>]
        attr_reader :query

        def initialize
            @query = Hash.new
        end

        # @api private
        #
        # Add a regular expression to {#query} so that self matches streams that
        # match either the current match or the new one
        #
        # @param [String] key the metadata key
        # @param [Regexp] rx the regular expression
        def add_regex(key, rx)
            if existing = query[key]
                query[key] = Regexp.union(existing, rx)
            else
                query[key] =  rx
            end
            self
        end

        # Match ports
        def ports
            add_regex('rock_stream_type', /^port$/)
        end

        # Match properties
        def properties
            add_regex('rock_stream_type', /^property$/)
        end

        # Match the object (port/property) name
        #
        # @param [String] name the object name to match
        def object_name(name)
            add_regex('rock_task_object_name', name)
        end

        # Match the task name
        #
        # @param [String] name the task name to match
        def task_name(name)
            add_regex('rock_task_name', name)
        end

        # Match the task model
        #
        # @param [Syskit::Models::TaskContext] model
        def task_model(model)
            add_regex('rock_task_model', model.orogen_model.name)
        end

        # Tests whether a stream matches this query
        #
        # @param [Pocolog::DataStream] stream
        # @return [Boolean]
        def ===(stream)
            query.all? do |key, matcher|
                if metadata = stream.metadata[key]
                    matcher === metadata
                end
            end
        end
    end
end


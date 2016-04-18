module Syskit::Pocolog
    # Exception raised in resolution methods when one match was expected but more
    # than one was found
    class Ambiguous < ArgumentError
    end

    # Exception raised in resolution methods when no information can allow to
    # infer the result
    class Unknown < ArgumentError
    end
end

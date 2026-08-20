# common_functions.sh
# Functions herin are intended to be used in scripts in the bin directory.

print_banner() {
    echo "|   ____    _    ____  ____  _   _ ___ ____  _____                "
    echo "|  / ___|  / \  |  _ \|  _ \| | | |_ _|  _ \| ____|               "
    echo "|  \___ \ / _ \ | |_) | |_) | |_| || || |_) |  _|                 "
    echo "|   ___) / ___ \|  __/|  __/|  _  || ||  _ <| |___                "
    echo "|  |____/_/   \_\_|   |_|   |_| |_|___|_| \_\_____|        _      "
    echo "|  |  ___|__  _ __ ___  ___ __ _ ___| |_  |_   _|__   ___ | |___  "
    echo "|  | |_ / _ \| '__/ _ \/ __/ _\` / __| __|   | |/ _ \ / _ \| / __| "
    echo "|  |  _| (_) | | |  __/ (_| (_| \__ \ |_    | | (_) | (_) | \__ \ "
    echo "|  |_|  \___/|_|  \___|\___\__,_|___/\__|   |_|\___/ \___/|_|___/ "
    echo "|                                                                 "
    echo "| Deploying the SAPPHIRE forecast tools ..."
    echo "| Date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
}

# This function takes a path and returns the last three elements of the path
keep_last_three_elements() {
    local path=$1
    local result=""

    for i in {1..3}; do
        result=$(basename "$path")/$result
        path=$(dirname "$path")
    done

    # Remove the trailing slash
    result=${result%/}
    echo "$result"
}

read_configuration(){
    echo "|       "
    echo "| ------"
    echo "| Reading configuration"
    echo "| ------"
    # If the argument is provided, write it to the environment variable
    # ieasyhydroforecast_env_file_path. If not, check if the environment variable
    # is set. If not, throw an error.
    if [ -n "$1" ];
    then
        env_file_path=$1
        # Derive the path to the .env file inside the container
        container_env_file_path=/$(keep_last_three_elements "$env_file_path")
        # Derive the path to the data reference directory within the container
        container_data_ref_dir=$(dirname "$container_env_file_path")
        container_data_ref_dir=$(dirname "$container_data_ref_dir")
        export ieasyhydroforecast_container_data_ref_dir=$container_data_ref_dir
        # Test if there is a ieasyhydroforecast_env_file_path variable set
        if [ -z "$ieasyhydroforecast_env_file_path" ];
        then
            # Test if the new env_file_path is different from the old one
            if [ "$ieasyhydroforecast_env_file_path" != "$env_file_path" ];
            then
                echo "| WARNING: Updating ieasyhydroforecast_env_file_path"
                echo "|    from $ieasyhydroforecast_env_file_path"
                echo "|    to $container_env_file_path"
            fi
        fi
        # For use by the forecast tools (inside docker containers) we need to know
        # the env file path inside the docker containers as well.
        export ieasyhydroforecast_env_file_path=$container_env_file_path
        echo "| Local path to .env read from argument: $env_file_path"
        echo "| Container path to .env derived: $ieasyhydroforecast_env_file_path"
        # Read the .env file
        if [ -f "$env_file_path" ]; then
            # Unset any dashboard origin variables inherited from a previous
            # read_configuration call in this same shell (operators are
            # instructed to source this file and call read_configuration
            # directly, see doc/prod/long_term_deploy_runbook.md:364-366, where
            # `source bin/utils/common_functions.sh` is at :364 and the
            # `read_configuration "<env>"` call is at :366).
            # Without this, the guard below cannot tell "this deployment did
            # not set it" from "a previous call in this shell exported it".
            unset ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad
            set -a  # Automatically export all variables
            source "$env_file_path"
            set +a  # Stop automatically exporting variables
        else
            echo "| .env file not found at $env_file_path!"
            exit 1
        fi

    else
        echo "| Error: No path to .env file was passed or was found in the environment!"
        exit 1
    fi

    # Derive ieasyhydroforecast_data_root_dir by removing the filename and 2 folder hierarchies
    ieasyhydroforecast_data_root_dir=$(dirname "$env_file_path")
    ieasyhydroforecast_data_ref_dir=$(dirname "$ieasyhydroforecast_data_root_dir")
    ieasyhydroforecast_data_root_dir=$(dirname "$ieasyhydroforecast_data_ref_dir")

    echo "| Local path to data reference directory: $ieasyhydroforecast_data_ref_dir"
    echo "| Container path to data reference directory: $container_data_ref_dir"
    echo "| ieasyhydroforecast_data_root_dir: $ieasyhydroforecast_data_root_dir"
    export ieasyhydroforecast_data_ref_dir
    export ieasyhydroforecast_data_root_dir

    # Export the docker image tag for the backend and frontend
    if [ -z "$ieasyhydroforecast_backend_docker_image_tag" ]; then
        echo "| WARNING: ieasyhydroforecast_backend_docker_image_tag is not set. Assuming 'local'"
        ieasyhydroforecast_backend_docker_image_tag="local"
    fi
    if [ -z "$ieasyhydroforecast_frontend_docker_image_tag" ]; then
        echo "| WARNING: ieasyhydroforecast_frontend_docker_image_tag is not set. Assuming 'local'"
        ieasyhydroforecast_frontend_docker_image_tag="local"
    fi
    export ieasyhydroforecast_backend_docker_image_tag
    export ieasyhydroforecast_frontend_docker_image_tag

    # Define subdomains for url, depending on the hm: 
    # 1. hm: "kyg" -> kyg.fc
    # 2. hm: "taj" -> taj.fc
    # kyg or taj are found in $env_file_path
    # If the last 4 characters of the env_file_path are 'kghm', we assume kyg, 
    # if they are 'tjhm', we assume taj.
    env_ending=${env_file_path: -4}
    tag=${ieasyhydroforecast_frontend_docker_image_tag}
    # An env file value for ieasyhydroforecast_url_pentad / _decad wins; the
    # ":=" form assigns only when the variable is unset OR empty, so an env
    # file line like "ieasyhydroforecast_url_pentad=" still falls back to the
    # derived value instead of producing an empty (Bokeh-crashing) origin.
    if [ "$env_ending" == "kghm" ]; then
        : "${ieasyhydroforecast_url_pentad:=kyg.fc.$ieasyhydroforecast_url}"
        : "${ieasyhydroforecast_url_decad:=demo.fc.decade.$ieasyhydroforecast_url}"
    elif [ "$env_ending" == "tjhm" ]; then
        : "${ieasyhydroforecast_url_pentad:=taj.fc.$ieasyhydroforecast_url}"
        : "${ieasyhydroforecast_url_decad:=taj.fc.decade.$ieasyhydroforecast_url}"
    elif [ "$env_ending" == "uzhm" ]; then
        : "${ieasyhydroforecast_url_pentad:=uzb.fc.$ieasyhydroforecast_url}"
        : "${ieasyhydroforecast_url_decad:=uzb.fc.decade.$ieasyhydroforecast_url}"
    else
        echo "| Error: Unknown hm in env_file_path: $env_file_path"
        exit 1
    fi
    export ieasyhydroforecast_url_pentad
    export ieasyhydroforecast_url_decad
    echo "| Resolved dashboard origins: pentad=$ieasyhydroforecast_url_pentad decad=$ieasyhydroforecast_url_decad"

    # If the env. varialbe ieasyhydroforecast_organization is not set, assume "demo"
    if [ -z "$ieasyhydroforecast_organization" ]; then
        echo "| WARNING: ieasyhydroforecast_organization is not set. Assuming 'demo'"
        ieasyhydroforecast_organization="demo"
    fi
    echo "| Deploying the SAPPHIRE forecast tools for organization:"
    echo "|    $ieasyhydroforecast_organization"

}

# Validate ieasyhydroforecast_url_pentad and ieasyhydroforecast_url_decad
# before a dashboard is started. NOT called from read_configuration (which
# has 38 call sites under bin/, several of them sourced into an operator's interactive
# shell) - call this only from the three dashboard launcher scripts. Those
# scripts are executed, but this function itself is also reachable if an
# operator sources common_functions.sh and calls it directly, so it uses
# `return 1`, not `exit 1` - callers that need fail-fast behaviour must use
# `validate_dashboard_origins || exit 1`.
#
# Each comma-separated entry must be HOST[:PORT] with no scheme, matching
# ^[A-Za-z0-9_.-]+(:[0-9]+)?$ - e.g. "host.example", "host_name",
# or "10.0.0.1:5006" - with the port (if present) in 1-65535. The HOST part
# is further rejected if it is all digits with no dot (a bare port with no
# host, e.g. "5006" typed instead of "HOST:5006") - see
# _check_dashboard_origin_value below. The entry must match the origin the
# BROWSER sends, not the port Panel listens on: for direct access that is
# Panel's own port (5006 pentad / 5007 decad), but behind a reverse proxy it
# is the proxy's external port, or no port at all.
#
# On success, both values are lowercased, have any leading zero stripped
# from a port, and are re-exported: Bokeh lowercases the browser-sent Origin
# header but compares it verbatim against the allow-list, so an uppercase
# entry ("HOST.EXAMPLE") or an unstripped leading zero ("host.example:05006"
# vs. an Origin of "host.example:5006") would otherwise silently match
# nothing.
#
# GENERALISED GUARD: any validator that TRANSFORMS its input after checking
# it - lowercasing, stripping - can manufacture a bad value from a value
# that was good at check time, and still report success, unless the
# transform's own output is checked too. A transform needs its own
# verification step; passing the pre-transform checks proves nothing about
# what the transform produces. This function has had multiple separate
# fail-open defects found in exactly this shape (a check that passed before
# a transform, silently invalidated by the transform, still returning 0):
# an empty value from a missing `tr`, an unchecked `tr` exit status, and an
# unchecked `printf -v` assignment. The durable fix is not to enumerate
# every way a transform can fail - it is to ASSERT THE POSTCONDITION: after
# every transform, re-validate what the variable actually now holds, not
# what it was expected to hold. See _check_dashboard_origin_value and its
# second call site below - do not remove that second call as "redundant
# with validation above"; it is checking a DIFFERENT value (the
# post-transform one) that the pre-transform check never saw.
_check_dashboard_origin_value() {
    # Validate the STRUCTURE of a single (var_name, value) pair: no embedded
    # newline/CR, not blank/whitespace-only, no leading/trailing/doubled
    # comma, and every comma-separated entry matches HOST[:PORT] with a
    # well-formed port (1-5 digits, 1-65535) and a host that is not itself a
    # bare port number. Prints an operator-facing error and returns 1 on the
    # first problem found; returns 0 if the whole value is structurally
    # valid. Called on both the raw input and the post-normalisation output
    # - see the GENERALISED GUARD comment above.
    local var_name=$1
    local value=$2
    local regex='^[A-Za-z0-9_.-]+(:([0-9]+))?$'
    local entry entries line port host

    if [[ "$value" == *$'\n'* || "$value" == *$'\r'* ]]; then
        echo "| Error: $var_name contains a newline or carriage return, which is not allowed in '$value'. Entries must be a single-line, comma-separated list of HOST[:PORT] with no scheme."
        return 1
    fi
    # Reject a whitespace-only (or empty) value outright, with a clear
    # message - splitting it would otherwise just produce an entry that
    # fails the regex below without saying why.
    if [ -z "${value// /}" ]; then
        echo "| Error: $var_name is empty or whitespace-only ('$value'). Entries must be HOST[:PORT] with no scheme, comma-separated; the entry must match the origin the browser sends, not the port Panel listens on."
        return 1
    fi
    # Reject a leading, trailing, or doubled comma explicitly - these would
    # otherwise split into a silent empty entry.
    if [[ "$value" == ,* || "$value" == *, || "$value" == *,,* ]]; then
        echo "| Error: $var_name contains an empty entry (leading, trailing, or doubled comma) in '$value'. Entries must be HOST[:PORT] with no scheme, comma-separated; the entry must match the origin the browser sends, not the port Panel listens on."
        return 1
    fi
    # Split on comma, reading line by line rather than relying on a single
    # IFS read <<< "$value" (which only ever sees the first line) - defense
    # in depth alongside the newline rejection above, so the split itself
    # never silently drops content past a line boundary even if a newline
    # reached this point some other way.
    entries=()
    while IFS= read -r line; do
        local -a line_entries=()
        IFS=',' read -ra line_entries <<< "$line"
        entries+=("${line_entries[@]}")
    done <<< "$value"
    # Fail closed if the split above produced NOTHING, rather than silently
    # returning success having inspected zero entries. On bash 3.2, a
    # here-string (`<<< "$value"`) needs a writable temp directory; where
    # none is available bash prints "cannot create temp file for here
    # document" and the loop body never runs, leaving `entries` empty even
    # though `$value` passed the non-blank check above. `$value` is
    # guaranteed non-blank at this point, so an empty `entries` here can
    # only mean the split itself failed, not that there was nothing to
    # validate.
    if [ ${#entries[@]} -eq 0 ]; then
        echo "| Error: $var_name could not be parsed into any entries from '$value' - the comma-split produced nothing even though the value is non-blank. This can happen if a here-document cannot be created in this environment (e.g. no writable temp directory on bash 3.2). Treating this as invalid rather than returning success having checked nothing."
        return 1
    fi
    for entry in "${entries[@]}"; do
        if ! [[ "$entry" =~ $regex ]]; then
            echo "| Error: $var_name has an invalid entry '$entry' in '$value'. Entries must be HOST[:PORT] with no scheme, comma-separated; the entry must match the origin the browser sends, not the port Panel listens on."
            return 1
        fi
        port=${BASH_REMATCH[2]}
        host=${entry%%:*}
        # FIX 5 (INFRA-032 review round 2): reject a host that is empty or
        # ends in a trailing dot with nothing after it, e.g. "kyg.fc." -
        # reachable when ieasyhydroforecast_url is unset and the derivation
        # in read_configuration concatenates a prefix with nothing after it
        # (see the "kyg.fc.$ieasyhydroforecast_url" derivation above).
        # DECISION: a trailing-dot FQDN (e.g. "host.example.") is legal DNS
        # (it denotes an absolute name), but it is rejected HERE anyway,
        # because the check that matters for this allow-list is not DNS
        # validity - it is whether the entry can ever equal a real browser
        # Origin header. Browsers never send a root-zone trailing dot in the
        # Origin header, so a trailing-dot entry can never match a real
        # request either way; treating it as a probable derivation bug (an
        # empty ieasyhydroforecast_url) is more useful to an operator than
        # silently exporting a dead origin.
        if [ -z "$host" ] || [[ "$host" == *. ]]; then
            echo "| Error: $var_name has an entry '$entry' in '$value' whose host '$host' is empty or ends in a trailing dot with nothing after it. This usually means a derived value was built from an unset variable (e.g. ieasyhydroforecast_url unset, producing 'kyg.fc.' with no host appended). A trailing-dot host is rejected even though it is legal DNS syntax, because the browser's Origin header never includes a root-zone trailing dot, so such an entry could never match a real request."
            return 1
        fi
        # FIX B: reject a host component that is ALL DIGITS with no dot -
        # that is a bare PORT with no host, e.g. "5006" (a plausible
        # operator typo for "HOST:5006"), not a hostname. An IPv4 literal
        # such as "10.0.0.1" is also all digits, but it contains dots, so it
        # is exempt. With no colon in the entry, $host is the whole entry.
        # Verified against the pinned Bokeh: an entry like "5006" produces
        # the allow-list entry "5006:80", which no browser Origin header can
        # ever match.
        if [[ "$host" =~ ^[0-9]+$ ]]; then
            echo "| Error: $var_name has an entry '$entry' in '$value' whose host '$host' is all digits with no dot - this looks like a bare port number, not a hostname. Did you mean 'HOST:$host'?"
            return 1
        fi
        if [ -n "$port" ]; then
            # Constrain the port's LENGTH/FORM before any arithmetic
            # comparison. $regex above already requires digits only, but
            # places no limit on how MANY digits - so a port with ~20+
            # digits reaches `[ "$port" -lt 1 ]` below, bash cannot parse it
            # as an integer, `[` emits "integer expression expected" on
            # BOTH the `-lt` and `-gt` comparisons, both evaluate false, and
            # (verified) the value is silently ACCEPTED. Reject on shape
            # first, with a check bash can always evaluate regardless of the
            # value's size.
            if ! [[ "$port" =~ ^[0-9]{1,5}$ ]]; then
                echo "| Error: $var_name has an invalid entry '$entry' in '$value': port '$port' is not a valid port number (must be 1-5 digits, 1-65535)."
                return 1
            fi
            if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
                echo "| Error: $var_name has an invalid entry '$entry' in '$value': port $port is out of range (must be 1-65535)."
                return 1
            fi
        fi
    done
    return 0
}

# FIX 4 (INFRA-032 review round 2): require var_name to be a plain scalar
# variable before validate_dashboard_origins reads it with `${!var_name}`.
# Env files are sourced as shell code (`source "$env_file_path"` in
# read_configuration), so a line like
# `declare -a ieasyhydroforecast_url_pentad=(host.example)` (an array) or,
# on bash 4.3+, `declare -n ieasyhydroforecast_url_pentad=some_other_var`
# (a nameref) is reachable, not hypothetical. `${!var_name}` on an array
# reads element zero, so validation, normalisation, and the final `export`
# would all silently operate on element zero (or the nameref's target)
# while returning 0 - the function reports success and the container still
# receives no real origin. `declare -p` prints the variable's attribute
# flags (e.g. `declare -ax name=...`); reject anything carrying `a`
# (indexed array), `A` (associative array), or `n` (nameref).
_require_scalar_dashboard_origin_var() {
    local var_name=$1
    local decl flags

    decl=$(declare -p "$var_name" 2>/dev/null) || {
        echo "| Error: $var_name is not set."
        return 1
    }
    if [[ "$decl" =~ ^declare\ -([a-zA-Z]+)\  ]]; then
        flags=${BASH_REMATCH[1]}
        if [[ "$flags" == *a* || "$flags" == *A* || "$flags" == *n* ]]; then
            echo "| Error: $var_name must be a plain scalar variable, not an array or nameref (declare flags: -$flags, from: $decl). An array/nameref definition for this name would silently validate and export only element zero (or the nameref's target), leaving the container with no real origin."
            return 1
        fi
    fi
    return 0
}

validate_dashboard_origins() {
    local var_name value

    for var_name in ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad; do
        _require_scalar_dashboard_origin_var "$var_name" || return 1
        value=${!var_name}
        _check_dashboard_origin_value "$var_name" "$value" || return 1
    done

    # Lowercase and normalise both values now that they have passed
    # validation above. Lowercase INTO A TEMPORARY VARIABLE first and check
    # BOTH the command's exit status AND its output before assigning the
    # real variable: if `tr` is not on PATH (cron strips PATH; a
    # minimal/coreutils-less container), `printf ... | tr ...` yields EMPTY
    # regardless of input; if `tr` fails in some other way that still
    # echoes its input back unchanged (e.g. a broken/partial coreutils),
    # the exit-status check catches that even though the output is
    # non-empty.
    for var_name in ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad; do
        value=${!var_name}
        local lowered lowered_status
        lowered=$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')
        lowered_status=$?
        if [ "$lowered_status" -ne 0 ]; then
            echo "| Error: lowercasing $var_name failed (tr exited $lowered_status) for input '$value'. PATH=$PATH - check that coreutils is installed and PATH has not been stripped (e.g. by cron)."
            return 1
        fi
        if [ -z "$lowered" ]; then
            echo "| Error: lowercasing $var_name produced an empty value from a non-empty input ('$value'). This usually means 'tr' is missing or failing on PATH (PATH=$PATH) - check that coreutils is installed and PATH has not been stripped (e.g. by cron)."
            return 1
        fi

        # Strip a leading zero from each entry's port, e.g.
        # "host.example:05006" -> "host.example:5006". Bokeh compares the
        # port as a STRING against the browser's Origin header, so an
        # unstripped leading zero never matches a real Origin - the same
        # failure family as the uppercase case above. A port that validated
        # as "0" was already rejected by the range check above, so this
        # loop never has to decide whether to normalise "0" itself down to
        # an empty string.
        local entries entry line
        entries=()
        while IFS= read -r line; do
            local -a norm_line_entries=()
            IFS=',' read -ra norm_line_entries <<< "$line"
            entries+=("${norm_line_entries[@]}")
        done <<< "$lowered"
        local -a normalized_entries=()
        local host port stripped_port
        for entry in "${entries[@]}"; do
            if [[ "$entry" == *:* ]]; then
                host=${entry%%:*}
                port=${entry#*:}
                stripped_port=$port
                while [ ${#stripped_port} -gt 1 ] && [ "${stripped_port:0:1}" = "0" ]; do
                    stripped_port=${stripped_port:1}
                done
                normalized_entries+=("${host}:${stripped_port}")
            else
                normalized_entries+=("$entry")
            fi
        done
        local normalized
        normalized=$(IFS=,; echo "${normalized_entries[*]}")
        if [ -z "$normalized" ]; then
            echo "| Error: normalising $var_name produced an empty value from a non-empty input ('$value')."
            return 1
        fi

        # FIX 2 (INFRA-032 review round 2): assert the value is CANONICAL,
        # not merely well-formed. `_check_dashboard_origin_value`'s regex
        # accepts uppercase too (it only checks STRUCTURE), so a `tr` that
        # exits 0 without actually lowercasing (reproduced: an environment
        # where `tr '[:upper:]' '[:lower:]'` is a locale-dependent no-op)
        # sails straight through both the exit-status check and the
        # structural re-check below, still holding e.g.
        # "HOST.EXAMPLE:5006" - and Bokeh compares the allow-list entry
        # verbatim against the (lowercased-by-Bokeh) browser Origin, so an
        # uppercase entry silently matches nothing.
        #
        # The durable way to assert "this is really lowercase" without
        # calling the (possibly broken) `tr` a second time - which would
        # prove nothing, since a no-op `tr` is trivially "idempotent" on
        # itself - is to check the POSTCONDITION a correct lowercasing must
        # satisfy: the result must be a fixed point of case-folding, i.e.
        # it must contain no ASCII uppercase character at all. This is a
        # bash builtin regex match (`[[ =~ ]]`), not an external tool, so it
        # cannot be defeated by the same failure mode it exists to catch.
        if [[ "$normalized" =~ [A-Z] ]]; then
            echo "| Error: normalising $var_name did not lowercase the value - '$normalized' still contains an uppercase character after the lowercasing step. This usually means 'tr' is not functioning as expected (PATH=$PATH, locale LC_ALL=${LC_ALL:-unset} LANG=${LANG:-unset}) - check that coreutils is installed, PATH has not been stripped (e.g. by cron), and the locale is not causing '[:upper:]'/'[:lower:]' to be a no-op."
            return 1
        fi

        local assign_status
        printf -v "$var_name" '%s' "$normalized"
        assign_status=$?
        if [ "$assign_status" -ne 0 ]; then
            echo "| Error: assigning the normalised value to $var_name failed (printf -v exited $assign_status) - the variable may be read-only or otherwise unassignable."
            return 1
        fi
        # Belt-and-suspenders alongside the exit-status check above: on at
        # least one bash this repo must run under (macOS system bash 3.2,
        # verified), `printf -v` on a READ-ONLY target prints "readonly
        # variable" to stderr but still returns exit status 0, so the check
        # above cannot be relied on alone to catch that case on every bash.
        # Comparing the variable's actual post-assignment content against
        # what was just assigned is exit-status-independent and catches a
        # silently-ignored assignment regardless of what `$?` reports.
        if [ "${!var_name}" != "$normalized" ]; then
            echo "| Error: assigning the normalised value to $var_name did not take effect - it now holds '${!var_name}', expected '$normalized', even though printf -v reported success. The variable may be read-only in a way this bash version does not report via its exit status."
            return 1
        fi

        # ASSERT THE POSTCONDITION. Re-run the same structural check used on
        # the raw input, but against the value $var_name now ACTUALLY HOLDS
        # (via `${!var_name}`, not against $normalized) - this is the
        # PRIMARY defence described in the GENERALISED GUARD comment above:
        # it catches a transform (here, or any future change to this
        # normalisation logic) that produces a structurally invalid value
        # (e.g. a broken `tr` that exits 0 but emits ",,," or an
        # oversized port) even when every individual step above reported
        # success. Do NOT remove this re-check as "we already validated the
        # input" - the input is not what gets exported; the POST-TRANSFORM
        # value is, and this function has repeatedly shipped bugs where the
        # two silently diverged.
        if ! _check_dashboard_origin_value "$var_name" "${!var_name}"; then
            echo "| Error: normalisation of $var_name produced an invalid value ('${!var_name}') from valid input ('$value'). This indicates a bug in the lowercasing/port-stripping logic, not a problem with the operator's input."
            return 1
        fi
    done
    # Both variables were already `export`-ed by read_configuration; re-export
    # them explicitly (rather than `export "$var_name"` inside the loop
    # above) so the exported name is a literal shellcheck can verify.
    export ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad
    # This is the value the container actually receives - report it here,
    # not just in read_configuration's earlier "Resolved dashboard origins"
    # line, because this normalisation happens after that line runs.
    echo "| Dashboard WebSocket origins in use (validated, lowercased, port-normalised): pentad=$ieasyhydroforecast_url_pentad decad=$ieasyhydroforecast_url_decad"
}

# Function to remove all Docker containers and images
clean_out_docker_space() {
    echo "|      "
    echo "| ------"
    echo "| Removing all containers and images"
    echo "| ------"
    # Take down the frontend if it is running
    docker compose -f bin/docker-compose-dashboards.yml down
    ieasyhydroforecast_data_root_dir=$ieasyhydroforecast_data_root_dir source ./bin/utils/clean_docker.sh --execute
}

# Function to stop and remove containers matching a name pattern
# Uses container IDs to handle dynamic container names (e.g., prepgateway_attempt_*)
stop_and_remove_container() {
    name_pattern=$1
    # Stop running containers matching the pattern
    for cid in $(docker ps -q -f name=$name_pattern); do
        docker stop $cid 2>/dev/null || true
    done
    # Remove all containers (running or stopped) matching the pattern
    for cid in $(docker ps -a -q -f name=$name_pattern); do
        docker rm $cid 2>/dev/null || true
    done
}

# Function to clean out the backend for res-running of the forecasts
clean_out_backend() {
    echo "|      "
    echo "| ------"
    echo "| Removing backend containers"
    echo "| ------"
    echo "| Removing all superfluous containers from the backend..."
    docker compose -f bin/docker-compose-luigi.yml down

    # List all containers that may be called in the pipeline
    stop_and_remove_container preprunoff
    stop_and_remove_container prepgateway
    stop_and_remove_container linreg
    stop_and_remove_container postprocessing_attempt
    stop_and_remove_container ml_TIDE_PENTAD
    stop_and_remove_container ml_TIDE_DECAD
    stop_and_remove_container ml_TFT_PENTAD
    stop_and_remove_container ml_TFT_DECAD
    stop_and_remove_container ml_TSMIXER_PENTAD
    stop_and_remove_container ml_TSMIXER_DECAD
    stop_and_remove_container ml_ARIMA_PENTAD
    stop_and_remove_container ml_ARIMA_DECAD
    stop_and_remove_container conceptmod
}

# Function to pull Docker images for the forecast tools
pull_docker_images() {
    echo "|      "
    echo "| ------"
    echo "| Pulling images"
    echo "| ------"

    # Pull (deployment mode)
    echo "| Pulling with TAG=$ieasyhydroforecast_backend_docker_image_tag"
    source ./bin/utils/pull_docker_images.sh $ieasyhydroforecast_backend_docker_image_tag
}

# Function to establish an SSH tunnel to the iEasyHydro (HF) server
establish_ssh_tunnel() {
    local ssh_to_ieh
    ssh_to_ieh="$(printf '%s' "${ieasyhydroforecast_ssh_to_iEH:-}" | tr '[:upper:]' '[:lower:]')"
    echo "| ieasyhydroforecast_ssh_to_iEH: ${ieasyhydroforecast_ssh_to_iEH:-}"

    # Check if SSH tunnel is required
    if [ "$ssh_to_ieh" != "true" ]; then
        echo "| SSH tunnel not required (ieasyhydroforecast_ssh_to_iEH is not set to true)"
        return 0
    fi

    echo "|      "
    echo "| ------"
    echo "| Establishing SSH tunnel to iEasyHydro server"
    echo "| ------"

    echo "| Establishing SSH tunnel to SAPPHIRE server..."
    source $ieasyhydroforecast_data_ref_dir/bin/.ssh/open_ssh_tunnel.sh
    wait  # Wait for the tunnel to be established

}

# Function to start the Docker container to re-set the run date
start_docker_container_reset_run_date() {
  echo "| Starting Docker container to re-set the run date ..."
  docker run -d \
    -e SAPPHIRE_OPDEV_ENV=True \
    --name resetrundate \
    --network host \
    -v $ieasyhydroforecast_data_ref_dir/config:/sensitive_data_forecast_tools/config \
    -v $ieasyhydroforecast_data_ref_dir/intermediate_data:/sensitive_data_forecast_tools/intermediate_data \
    mabesa/sapphire-rerun:latest
  echo "| Docker container started with name resetrundate"
}


# Function to start the Docker Compose service for the backend pipeline
start_docker_compose_luigi() {
    local service_name=$1
    local sapphire_prediction_mode=$2

    echo "|      "
    echo "| ------"
    echo "| Starting backend services"
    echo "| ------"
    echo "| Starting Docker Compose service for backend ..."

    if [ -n "$service_name" ]; then
        if [ -n "$sapphire_prediction_mode" ]; then
            export SAPPHIRE_PREDICTION_MODE="$sapphire_prediction_mode"
        fi
        echo "| Starting Docker Compose service for backend: $service_name with prediction mode $SAPPHIRE_PREDICTION_MODE..."
        SAPPHIRE_PREDICTION_MODE="$SAPPHIRE_PREDICTION_MODE" docker compose -f bin/docker-compose-luigi.yml up -d "$service_name" &
    else
        echo "| Starting all Docker Compose services for backend ..."
        docker compose -f bin/docker-compose-luigi.yml up -d &
    fi

    DOCKER_COMPOSE_LUIGI_PID=$!
    echo "| Docker Compose service started with PID $DOCKER_COMPOSE_LUIGI_PID"
}

# Function to start the Docker Compose service for the dashboards
start_docker_compose_dashboards() {
    echo "|      "
    echo "| ------"
    echo "| Starting frontend services"
    echo "| ------"
    echo "| Starting Docker Compose service for the dashboards..."
    echo "| Deploying dashboard to: ieasyhydroforecast_url: $ieasyhydroforecast_url"
    echo "| Inside the container, the path to the .ssh directory is: $ieasyhydroforecast_container_data_ref_dir/bin/.ssh"
    ieasyhydroforecast_url_pentad=$ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad=$ieasyhydroforecast_url_decad ieasyhydroforecast_frontend_docker_image_tag=$ieasyhydroforecast_frontend_docker_image_tag ieasyhydroforecast_container_data_ref_dir=$ieasyhydroforecast_container_data_ref_dir docker compose -f sapphire/docker-compose.yml up -d &
    DOCKER_COMPOSE_DASHBOARD_PID=$!
    echo "| Docker Compose service started with PID $DOCKER_COMPOSE_DASHBOARD_PID"
}

# Clean up processes on script exit (used with trap)
cleanup() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up"
  echo "| ------"
  if [ -n "${ieasyhydroforecast_ssh_tunnel_pid:-}" ]; then
    kill "$ieasyhydroforecast_ssh_tunnel_pid" 2>/dev/null || true
  fi
}

# Clean up processes on script exit (used with trap)
cleanup_deployment() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up"
  echo "| ------"
  if [ -n "${ieasyhydroforecast_ssh_tunnel_pid:-}" ]; then
    kill "$ieasyhydroforecast_ssh_tunnel_pid" 2>/dev/null || true
  fi
  echo "|       "
  echo "| ------"
  echo "|       "
  echo "| You have now run the SAPPHIRE forecast tools for the first time!"
  echo "|       "
  echo "| Next steps (follow the docs for more detailed instructions):"
  echo "| 1. Check the logs of the Docker Compose service for any errors."
  echo "| 2. Check if the dashboards are running and displaying as expected."
  echo "| 3. Set up cron jobs for the dashboard services and for the daily run of the forecasting pipeline."
  echo "| "
}

cleanup_preprocessing_containers() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up preprocessing containers"
  echo "| ------"
  stop_and_remove_container preprunoff
  stop_and_remove_container prepgateway
}

cleanup_decadal_forecasting_containers() {
  echo "|      "
  echo "| ------"
  echo "| Cleaning up decadal forecasting containers"
  echo "| ------"
  stop_and_remove_container ml_TIDE_DECAD
  stop_and_remove_container ml_TFT_DECAD
  stop_and_remove_container ml_TSMIXER_DECAD
  stop_and_remove_container ml_ARIMA_DECAD
  stop_and_remove_container linreg
  stop_and_remove_container conceptmod
  stop_and_remove_container postprocessing_attempt
}

cleanup_pentadal_forecasting_containers() {
    echo "|      "
    echo "| ------"
    echo "| Cleaning up pentadal forecasting containers"
    echo "| ------"
    stop_and_remove_container ml_TIDE_PENTAD
    stop_and_remove_container ml_TFT_PENTAD
    stop_and_remove_container ml_TSMIXER_PENTAD
    stop_and_remove_container ml_ARIMA_PENTAD
    stop_and_remove_container linreg
    stop_and_remove_container conceptmod
    stop_and_remove_container postprocessing_attempt
}

cleanup_long_term_forecasting_containers() {
    echo "|      "
    echo "| ------"
    echo "| Cleaning up long-term forecasting containers"
    echo "| ------"
    stop_and_remove_container lt_schedule_query
    stop_and_remove_container lt_forecast
    stop_and_remove_container lt-postprocessing
}

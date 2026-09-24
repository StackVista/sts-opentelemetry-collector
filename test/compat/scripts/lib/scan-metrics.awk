# Scan Go source for metric name literals and their submission kind.
#
# Inputs (awk -v):
#   check     label written to the output rows
#   family    metric prefix accepted as a whole name, for example "system."
#   implicit  prefix prepended to bare suffix literals, for example "kubernetes." for the
#             kubelet providers. Empty disables it.
#
# Output: metric,check,kind,source
#
# A bare suffix literal is only prefixed when it appears in a context that makes it a metric
# name: concatenated onto a prefix constant, passed to a submission helper, or sitting on the
# right hand side of a metric name map. Without that restriction any unrelated dotted string
# in the tree becomes a metric.
#
# Kind detection uses a three-line window: a name literal and a submission token within three
# lines of each other are assumed to belong together. That is true for the providers in this
# tree, but it is a heuristic and "unknown" is reported rather than guessed.
#
# Two families append a suffix to a name at submission time and cannot be seen as literals:
# the protocol counters that also emit a ".count" series, and the histogram-derived kubelet
# durations that emit ".sum" and ".count". Both are expanded from the map that holds the base
# names, so the expansion follows the source when names are added or removed.

function count_char(s, c,   n, i) {
    n = 0
    for (i = 1; i <= length(s); i++) if (substr(s, i, 1) == c) n++
    return n
}

function emit_file(    i, m, k) {
    for (i = 1; i <= npending; i++) {
        m = pending_name[i]
        k = (pending_kind[i] == "" ? "unknown" : pending_kind[i])
        printf "%s,%s,%s,%s\n", m, check, k, shortfile
    }
    npending = 0
}

function record(name, lineno, kind,    i) {
    for (i = 1; i <= npending; i++) {
        if (pending_name[i] == name) {
            if (kind != "" && pending_kind[i] == "") pending_kind[i] = kind
            return
        }
    }
    npending++
    pending_name[npending] = name
    pending_kind[npending] = kind
    pending_line[npending] = lineno
}

function attach(kind, lineno,    i) {
    for (i = npending; i >= 1; i--) {
        if (lineno - pending_line[i] > 3) break
        if (pending_kind[i] == "") pending_kind[i] = kind
    }
}

FNR == 1 {
    emit_file()
    shortfile = FILENAME
    sub(/^.*\/corechecks\//, "", shortfile)
    mapname = ""
    depth = 0
}

{
    line = $0
    sub(/[[:space:]]*\/\/.*$/, "", line)

    # Enter a metric name map, so literals inside it can be recognised as names and the
    # suffix-expanding maps can be expanded.
    if (depth == 0 && match(line, /[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=[[:space:]]*map\[/)) {
        mapname = substr(line, RSTART, RLENGTH)
        sub(/[[:space:]]*=.*$/, "", mapname)
    }

    # Format-string constants, for example: diskMetric = "system.disk.%s"
    if (match(line, /[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=[[:space:]]*"[A-Za-z0-9_.]+%[sv]"/)) {
        decl = substr(line, RSTART, RLENGTH)
        cname = decl; sub(/[[:space:]]*=.*$/, "", cname)
        cval = decl; sub(/^[^"]*"/, "", cval); sub(/"$/, "", cval)
        if (index(cval, family) == 1) fmtconst[cname] = cval
    }

    # Uses of those constants, for example: fmt.Sprintf(diskMetric, "in_use")
    for (cname in fmtconst) {
        rest2 = line
        while (match(rest2, /Sprintf\([A-Za-z_][A-Za-z0-9_]*,[[:space:]]*"[A-Za-z0-9_.]+"/)) {
            call = substr(rest2, RSTART, RLENGTH)
            rest2 = substr(rest2, RSTART + RLENGTH)
            used = call; sub(/^Sprintf\(/, "", used); sub(/,.*$/, "", used)
            if (used != cname) continue
            arg = call; sub(/^[^"]*"/, "", arg); sub(/"$/, "", arg)
            resolved = fmtconst[cname]
            sub(/%[sv]/, arg, resolved)
            record(resolved, FNR, "")
        }
    }

    has_prefix_const = (line ~ /MetricsPrefix/)
    in_helper = (line ~ /(reportMetric|processContainerMetric|senderFunc|reportFsMetric|SubmitMetric)\(/)
    in_map_entry = (line ~ /"[A-Za-z0-9_]+"[[:space:]]*:[[:space:]]*"/)

    rest = line
    while (match(rest, /"[A-Za-z0-9_.]+"/)) {
        lit = substr(rest, RSTART + 1, RLENGTH - 2)
        rest = substr(rest, RSTART + RLENGTH)

        name = ""
        if (index(lit, family) == 1) {
            name = lit
        } else if (implicit != "" && lit ~ /^[a-z][a-z0-9_]*([.][a-z0-9_]+)+$/ &&
                   lit !~ /[.](json|yaml|yml|txt|go|sock)$/ &&
                   (has_prefix_const || in_helper || in_map_entry)) {
            name = implicit lit
        }

        if (name == "") continue
        if (name ~ /[.]$/ || name ~ /^[.]/ || index(name, "..") > 0) continue
        if (index(name, ".") == 0) continue

        record(name, FNR, "")

        # Suffix expansion, driven by the map the base name came from.
        if (mapname == "protocolsMetricsMapping") {
            record(name ".count", FNR, "monotonic_count")
        } else if (mapname == "transformValuesHistogram") {
            record(name ".sum", FNR, "")
            record(name ".count", FNR, "")
        }
    }

    kind = ""
    if (line ~ /[.]ServiceCheck[,(]/) kind = "service_check"
    else if (line ~ /[.]MonotonicCount[,(]/) kind = "monotonic_count"
    else if (line ~ /[.]Rate[,(]|"rate"/) kind = "rate"
    else if (line ~ /[.]Count[,(]/) kind = "count"
    else if (line ~ /[.]Gauge[,(]|"gauge"/) kind = "gauge"

    if (kind != "") attach(kind, FNR)

    depth += count_char(line, "{") - count_char(line, "}")
    if (depth <= 0) { depth = 0; mapname = "" }
}

END { emit_file() }

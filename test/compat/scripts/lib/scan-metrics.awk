# Scan Go source for metric name literals and their submission kind.
#
# Inputs (awk -v):
#   check     label written to the output rows
#   family    metric prefix accepted as a whole name, for example "system."
#   implicit  prefix prepended to bare suffix literals found inside submission helpers,
#             for example "kubernetes." for the kubelet providers. Empty disables it.
#
# Output: metric,check,kind,source
#
# Kind detection uses a three-line window: a name literal and a submission token within
# three lines of each other are assumed to belong together. That is true for the providers
# in this tree, where the name is assigned to a variable and submitted a line or two later,
# but it is a heuristic and "unknown" is reported rather than guessed.

function emit_file(    i, m, k) {
    for (i = 1; i <= npending; i++) {
        m = pending_name[i]
        k = (pending_kind[i] == "" ? "unknown" : pending_kind[i])
        printf "%s,%s,%s,%s\n", m, check, k, shortfile
    }
    npending = 0
}

function record(name, lineno,    i) {
    # Reuse the entry if this name was already seen in this file, so the kind attaches to
    # the closest occurrence rather than the first one.
    for (i = 1; i <= npending; i++) {
        if (pending_name[i] == name && pending_kind[i] != "") return
    }
    npending++
    pending_name[npending] = name
    pending_kind[npending] = ""
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
}

{
    line = $0
    sub(/[[:space:]]*\/\/.*$/, "", line)   # drop trailing comments

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
            record(resolved, FNR)
        }
    }

    has_prefix_const = (line ~ /MetricsPrefix/)
    in_helper = (line ~ /(reportMetric|processContainerMetric|senderFunc|reportFsMetric)\(/)

    rest = line
    while (match(rest, /"[A-Za-z0-9_.]+"/)) {
        lit = substr(rest, RSTART + 1, RLENGTH - 2)
        rest = substr(rest, RSTART + RLENGTH)

        name = ""
        if (index(lit, family) == 1) {
            name = lit
        } else if (implicit != "" && lit ~ /^[a-z][a-z0-9_]*([.][a-z0-9_]+)+$/ && lit !~ /[.](json|yaml|yml|txt|go|sock)$/) {
            name = implicit lit
        }

        if (name == "") continue
        if (name ~ /[.]$/ || name ~ /^[.]/ || index(name, "..") > 0) continue
        if (index(name, ".") == 0) continue

        record(name, FNR)
    }

    kind = ""
    if (line ~ /[.]ServiceCheck\(/) kind = "service_check"
    else if (line ~ /[.]MonotonicCount\(/) kind = "monotonic_count"
    else if (line ~ /[.]Rate\(|sender[.]Rate|"rate"/) kind = "rate"
    else if (line ~ /[.]Count\(/) kind = "count"
    else if (line ~ /[.]Gauge\(|sender[.]Gauge|"gauge"/) kind = "gauge"

    if (kind != "") attach(kind, FNR)
}

END { emit_file() }

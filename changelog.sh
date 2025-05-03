#!/bin/bash
git log --pretty=format:"%ad %h: %s (%an)" --date=short --no-merges | sort -r | awk -F' ' '

BEGIN {
    order[1] = "Features"
    order[2] = "Fixes"
    order[3] = "Changes"
    order[4] = "Other"
}
{
    current_date = $1
    hash = $2
    subject = ""
    for (i=3; i<=NF; i++) {
        subject = subject (i > 3 ? " " : "") $i
    }

    if (subject ~ /^(feat:|[Aa]dd|[Cc]reate)/) {
        type = "Features"
        sub(/^feat:[[:space:]]*/, "", subject)
    } else if (subject ~ /^(chore:|[Tt]est|[Uu]pdate)/) {
        type = "Changes"
        sub(/^change:[[:space:]]*/, "", subject)
    } else if (subject ~ /^(fix:|[Ff]ix)/) {
        type = "Fixes"
        sub(/^fix:[[:space:]]*/, "", subject)
    } else {
        type = "Other"
    }

    sub(/^[[:space:]]+/, "", subject)
    sub(/[[:space:]]+$/, "", subject)

    commit_line = "- " hash " " subject

    if (!dates[current_date]++) {
        dates_order[++date_count] = current_date
    }

    commits[current_date,type] = commits[current_date,type] (commits[current_date,type] ? "\n" : "") commit_line
    types[current_date,type] = 1
}
END {
    for (d = 1; d <= date_count; d++) {
        date = dates_order[d]
        print date "\n"
        # Iterate through our predefined order
        for (i = 1; i <= 4; i++) {
            t = order[i]
            if ((date, t) in types) {
                print t
                split(commits[date, t], lines, "\n")
                for (j = 1; j <= length(lines); j++) {
                    print lines[j]
                }
                print ""
            }
        }
        print ""
    }
}' > CHANGELOG.md

# Better alternatives: 
# - https://github.com/git-chglog/git-chglog
# - https://github.com/conventional-changelog/conventional-changelog
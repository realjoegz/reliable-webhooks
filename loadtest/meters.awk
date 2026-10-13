# What the meters of two replicas counted between two scrapes of each. The arguments are four
# scrapes of /actuator/prometheus: the first replica before and after, then the second replica
# before and after.
#
# Only meters that count are read. Of the service those are its counters and the counts, sums
# and buckets of its timers. Of what Spring Boot adds they are the requests that the web server
# answered, the time that a borrower waited for a connection of a pool, the pauses of the
# garbage collector and the CPU time of the process. A gauge, such as the largest value of a
# timer, has no increase. A meter that counted nothing is left out, and so is a bucket above the
# first one that holds everything its timer counted.
#
# Below the increases are the mean of each timer, which is the increase of its sum over the
# increase of its count, and the share of that count that each bucket holds. Both are of the
# two replicas together.

BEGIN {
  counted = "^(webhooks_|hikaricp_connections_(acquire_seconds|timeout)_" \
    "|http_server_requests_seconds_count|jvm_gc_pause_seconds_|process_cpu_time_ns_total)"
}

FNR == 1 {
  scrape++
}

# A line of a scrape is a meter with its labels, a space and a value. A label can hold a space.
$0 ~ counted {
  labelled = $0
  sub(/ [^ ]+$/, "", labelled)
  name = labelled
  sub(/\{.*/, "", name)
  if (name ~ /_(total|count|sum|bucket)$/) {
    if (!(labelled in seen)) {
      seen[labelled] = 1
      meter[++meters] = labelled
    }
    value[scrape, labelled] = $NF
  }
}

# A whole number is printed as one, however large: the CPU time of a process is in nanoseconds.
function number(n) {
  return n == int(n) ? sprintf("%.0f", n) : sprintf("%.3f", n)
}

# The count of the timer that a bucket or a sum belongs to.
function count_of(part) {
  sub(/_(bucket|sum)/, "_count", part)
  sub(/,?le="[^"]*"/, "", part)
  sub(/\{\}$/, "", part)
  return part
}

END {
  for (m = 1; m <= meters; m++) {
    first[m] = value[2, meter[m]] - value[1, meter[m]]
    second[m] = value[4, meter[m]] - value[3, meter[m]]
    both[meter[m]] = first[m] + second[m]
  }
  for (m = 1; m <= meters; m++) {
    if (meter[m] ~ /_bucket\{/) {
      count = count_of(meter[m])
      shown[m] = !(count in full)
      if (both[meter[m]] == both[count]) {
        full[count] = 1
      }
    } else {
      shown[m] = 1
    }
    shown[m] = shown[m] && both[meter[m]] != 0
  }

  row = "%14s %14s %14s  %s\n"
  printf row, "replica-1", "replica-2", "both", "increase during the scenario"
  for (m = 1; m <= meters; m++) {
    if (shown[m]) {
      printf row, number(first[m]), number(second[m]), number(both[meter[m]]), meter[m]
    }
  }

  printf "\n%14s  %s\n", "value", "of both replicas together"
  for (m = 1; m <= meters; m++) {
    if (shown[m] && meter[m] ~ /_sum(\{|$)/) {
      timer = meter[m]
      sub(/_sum/, "", timer)
      printf "%14.6f  mean of %s\n", both[meter[m]] / both[count_of(meter[m])], timer
    } else if (shown[m] && meter[m] ~ /_bucket\{/) {
      printf "%14.6f  share of %s\n", both[meter[m]] / both[count_of(meter[m])], meter[m]
    }
  }
}

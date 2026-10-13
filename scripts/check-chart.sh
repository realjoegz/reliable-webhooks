#!/usr/bin/env bash
#
# The chart without a cluster: helm lint, every manifest that the chart renders held against the
# schema of its kind, and values that the chart has to refuse. CI runs this ahead of
# scripts/kind-smoke.sh.
#
# Needs helm 4 and kubeconform, and the network: kubeconform downloads the schemas from
# raw.githubusercontent.com. CI runs it with Helm 4.3.0 and kubeconform 0.8.0. A refusal is
# recognised by the words that the schema validator of Helm 4 has for it. A Helm before 3.18.5
# has another validator, with other words for the same refusals.

set -euo pipefail
cd "$(dirname "$0")/.."

chart=deploy/helm/reliable-webhooks
# The values of the smoke test. They name the image, the database and the Secret, which the chart
# has no default for.
values=deploy/kind/values.yaml
# The five values that docs/deploy.md installs the chart with, as it gives them.
documented=(
  --set image.repository=registry.example.com/reliable-webhooks
  --set-string image.tag=1
  --set 'database.url=jdbc:postgresql://db.example.com:5432/webhooks?sslmode=require'
  --set database.username=webhooks
  --set secret.name=reliable-webhooks-credentials
)
# The kinds of every release, and those of the two templates that are off by default.
always="Deployment PodDisruptionBudget Service ServiceAccount"
on_request="HorizontalPodAutoscaler ServiceMonitor"

# Where kubeconform keeps the schemas that it has downloaded, so that the three renders of a run
# download each of them once.
schemas=$(mktemp -d)
trap 'rm -rf "$schemas"' EXIT

# Renders the chart with the given values and holds every manifest against the schema of its
# kind. The kinds have to be those of the first argument, no more and no fewer.
#
# The schemas are those of Kubernetes 1.37, which is the version of the smoke test, read from
# the repository that kubeconform reads them from by default. A ServiceMonitor is a resource of
# the Prometheus Operator, whose schema is read from the CRDs catalog of Datree. Both are read
# at a fixed commit. A manifest without a schema fails the check, and so does a schema that
# cannot be downloaded.
renders() {
  local expected manifests kinds
  expected=$(tr ' ' '\n' <<<"$1" | sort | paste -sd ' ' -)
  shift
  manifests=$(helm template reliable-webhooks "$chart" "$@")
  kubeconform -strict -verbose -summary -kubernetes-version 1.37.0 -cache "$schemas" \
    -schema-location 'https://raw.githubusercontent.com/yannh/kubernetes-json-schema/817113f21a2643311cbc3e992bb44ffe1c416c54/{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}/{{.ResourceKind}}{{.KindSuffix}}.json' \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/63669a570e231d4f1f8396d229a1de512bcf0a34/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
    <<<"$manifests"
  kinds=$(sed -n 's/^kind: //p' <<<"$manifests" | sort | paste -sd ' ' -)
  if [ "$kinds" != "$expected" ]; then
    echo "The chart renders $kinds where it has to render $expected: $*" >&2
    exit 1
  fi
}

# Ends the script unless the chart refuses to render with the given values, and says why in
# words that hold the first argument.
refuses() {
  local why=$1 said
  shift
  if said=$(helm template reliable-webhooks "$chart" "$@" 2>&1); then
    echo "The chart renders with values that it has to refuse: $*" >&2
    exit 1
  fi
  if ! grep --quiet --fixed-strings -- "$why" <<<"$said"; then
    printf 'The chart refuses these values for another reason than expected: %s\n%s\n' \
      "$*" "$said" >&2
    exit 1
  fi
  echo "Refused ($why): ${*:-the values of the chart alone}"
}

helm lint --strict "$chart" --values "$values"

echo "The chart as the smoke test installs it:"
renders "$always" --values "$values"
echo "The chart with the two templates that are off by default:"
renders "$always $on_request" \
  --values "$values" --set autoscaling.enabled=true --set serviceMonitor.enabled=true
echo "The chart as docs/deploy.md installs it:"
renders "$always" "${documented[@]}"

echo "Values that the chart has to refuse:"
for unset in /image/repository /image/tag /database/url /database/username /secret/name; do
  refuses "at '$unset'"
done
refuses "/image/tag': got number, want string" --values "$values" --set image.tag=1
refuses "/replicaCount" --values "$values" --set replicaCount=0
refuses "/database': additional properties 'password'" \
  --values "$values" --set database.password=in-plain-text
refuses "/database/url': 'not' failed" --values "$values" \
  --set "database.url=jdbc:postgresql://postgres:5432/webhooks?password=in-plain-text"
for name in WEBHOOKS_DATABASE_URL WEBHOOKS_DATABASE_USERNAME WEBHOOKS_DATABASE_PASSWORD \
  WEBHOOKS_ADMINTOKEN WEBHOOKS_SECRETENCRYPTIONKEY; do
  refuses "/extraEnv/0/name" \
    --values "$values" --set "extraEnv[0].name=$name" --set "extraEnv[0].value=x"
done
refuses "/service/type" --values "$values" --set service.type=LoadBalancer
refuses "a replica needs 30 s to drain" \
  --values "$values" --set terminationGracePeriodSeconds=34
refuses "incompatible with Kubernetes v1.29.0" --values "$values" --kube-version 1.29.0

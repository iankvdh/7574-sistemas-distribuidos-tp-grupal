#!/usr/bin/env bash
# Interactive scenario picker. Lists every scenarios/*.yaml available, lets the
# user choose one, regenerates it from its spec in scenarios/specs/ if present,
# and copies it to docker-compose.yaml at the repo root.

set -euo pipefail

cd "$(dirname "$0")/.."

mapfile -t SCENARIOS < <(find scenarios -maxdepth 1 -name "*.yaml" | sort)

if [ ${#SCENARIOS[@]} -eq 0 ]; then
  echo "No scenarios found under scenarios/. Run 'make gen-all' first." >&2
  exit 1
fi

echo "Escenarios disponibles:"
for i in "${!SCENARIOS[@]}"; do
  base=$(basename "${SCENARIOS[$i]}" .yaml)
  spec="scenarios/specs/${base}.yaml"
  marker=""
  if [ -f "$spec" ]; then
    marker=" (generated from specs/${base}.yaml)"
  fi
  printf "  %d) %s%s\n" "$((i + 1))" "${SCENARIOS[$i]}" "$marker"
done

read -r -p "Selecciona uno [1-${#SCENARIOS[@]}]: " choice
idx=$((choice - 1))
if [ -z "$choice" ] || ! [[ "$choice" =~ ^[0-9]+$ ]] || [ "$idx" -lt 0 ] || [ "$idx" -ge ${#SCENARIOS[@]} ]; then
  echo "Selección inválida." >&2
  exit 1
fi

chosen="${SCENARIOS[$idx]}"
base=$(basename "$chosen" .yaml)
spec="scenarios/specs/${base}.yaml"

if [ -f "$spec" ]; then
  echo "Regenerando ${chosen} desde ${spec}..."
  (cd src && go run ./tools/compose-gen "../${spec}" > "../${chosen}")
fi

cp "$chosen" docker-compose.yaml
echo "Listo. docker-compose.yaml apunta ahora a ${chosen}."

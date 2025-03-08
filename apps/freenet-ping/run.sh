ARGS=""
for arg in "$@"; do
  if [[ "$arg" == "put" ]]; then
    ARGS="--put-contract"
    break
  fi
done

RUST_LOG=info,freenet_ping=debug freenet-ping --host 127.0.0.1:$WS_PORT --frequency=2000ms --ttl=7200s --tag=$TAG --code-key=$CODE_KEY $ARGS

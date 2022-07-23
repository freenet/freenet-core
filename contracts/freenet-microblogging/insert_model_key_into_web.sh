timeout 3 cargo run --example contract_browsing --features local 2> LOCUTUS_TMP | sleep 3
LOCUTUS_BLOG_MODEL_KEY=$(cat LOCUTUS_TMP | grep -o 'loading model contract \w*' | grep -o '\w*$')
cat view/state.html | sed "s/let MODEL_CONTRACT = .*/let MODEL_CONTRACT = '$LOCUTUS_BLOG_MODEL_KEY';/g" > ./view/web/state.html
unset LOCUTUS_BLOG_MODEL_KEY
rm LOCUTUS_TMP 

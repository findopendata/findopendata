SOURCE="${BASH_SOURCE[0]}"
APP_DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
CODE_DIR="$( cd -P "$( dirname "$APP_DIR" )" >/dev/null 2>&1 && pwd )"

cp -r "$CODE_DIR/crawler/parsers" "$APP_DIR/parsers"

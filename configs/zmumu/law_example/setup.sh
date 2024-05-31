# setup PATHs and configurations for law

action() {
    # get important paths
    orig="${PWD}"
    this_file="$( [ ! -z "${ZSH_VERSION}" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    cd "${orig}"

    # add directory to pythonpath, so modules can be imported
    export PYTHONPATH="${this_dir}:${PYTHONPATH}"

    # === law ===
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export LAW_HOME="${this_dir}/.law"

    # source law's bash completion script
    if which law &> /dev/null; then
        source "$( law completion )" ""

        # index law and check if it was successful
        law index -q
        return_code=$?
        if [ ${return_code} -ne 0 ]; then
            echo "failed to index law with error code ${return_code}"
            return 1
        else
            echo "law tasks were successfully indexed"
        fi

    fi

}

action "$@"
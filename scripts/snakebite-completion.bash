_snakebite()
{
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    #echo "CUR=|${cur}| PREV=|${prev}|"

    if [ ${prev} != "snakebite" ] ; then
        num_slashes=$(echo $cur | grep -o "/" | wc -l)
        level=`expr $num_slashes - 1`
        if [ $level -lt 0 ]; then
            level=0
        fi

        last_root=$(echo $cur | grep -o "^/\([^/]*/\)*")
        opts=$(snakebite $prev $last_root |grep -v Found | awk '{dir = (substr($1,0,1) == "d"); if(dir){print $8 "/"} else { print $8}}')

        if [[ ${cur} == * ]] ; then
            COMPREPLY=($(compgen -W "${opts}" -- ${cur}))
            [[ $COMPREPLY = */ ]] && compopt -o nospace
            return 0
        fi
    else
        opts=$(snakebite commands)
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    fi
}
complete -F _snakebite snakebite
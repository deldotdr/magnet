ATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON=/usr/sbin/rabbitmq-multi
NAME=rabbitmq-server
DESC=rabbitmq-server
USER=rabbitmq
NODE_COUNT=1


start_rabbitmq () {
      set +e
      $DAEMON start_all ${NODE_COUNT} > /var/log/rabbitmq/startup.log 2> /var/log/rabbitmq/startup.err
      case "$?" in
        0)
          echo SUCCESS;;
        1)
          echo TIMEOUT - check /var/log/rabbitmq/startup.\{log,err\};;
        *)
          echo FAILED - check /var/log/rabbitmq/startup.log, .err
          exit 1;;
      esac
      set -e
}

start_rabbitmq

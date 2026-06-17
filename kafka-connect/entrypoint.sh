#!/bin/bash

#
# Kafka Connect custom entrypoint script
#
# This script takes care of automatically installing plugins and deploying connectors when Kafka Connect
# is first started. The script looks for the following files:
#
# plugins - a plain text file listing (one per #uncommented line) the plugins to install, which are either
#           plugin names on Confluent Hub (e.g., confluentinc/kafka-connect-jdbc:10.7.0) or relative paths
#           to local folders containing the code of a plugin (e.g., myplugin-dir, with myplugin-dir located
#           in the same parent folder of this script)
#
# *.json  - each file correspond to a connector to be deployed, and has to be named as the connector,
#           e.g., myconnector.json will contain the JSON for connector with 'name': 'myconnector'
#
# There should be no need to edit this script. If you do that, be careful not to use a Windows editor using
# CR/LF line endings, as then the script may not run properly.
#

# Move to the directory where the script is placed, where we look for 'plugins' and connector '.json' files
shopt -s nullglob
cd "$( dirname -- "${BASH_SOURCE[0]}" )" || exit

# At first run & if file 'plugins' exists, run 'confluent-hub install --no-prompt' on each non-commented file line
if [ ! -f /usr/share/confluent-hub-components/plugins-installed ] && [ -f plugins.txt ]; then
	touch /usr/share/confluent-hub-components/plugins-installed
	echo "INIT: processing plugins"
	grep -vE '^\s*(#.*)?$' plugins.txt | sed -E "s/\r//g" | { # sed used to handle Windows \r\n line endings
		while read -r plugin_name; do
			if [ -d "$plugin_name" ]; then
				echo "INIT: installing $plugin_name from local directory"
				ln -s "$( pwd )/$plugin_name" "/usr/share/confluent-hub-components/$plugin_name"
			elif [[ "$plugin_name" == http://*.zip ]] || [[ "$plugin_name" == https://*.zip ]]; then
				echo "INIT: installing $plugin_name from URL"
				plugin_url="$plugin_name"
				plugin_name="$( echo "$plugin_url" | sed -E 's|.*/([^/]+).zip|\1|')"
				(
					mkdir "/usr/share/confluent-hub-components/$plugin_name"
					cd "/usr/share/confluent-hub-components/$plugin_name" || exit
					wget -O "${plugin_name}.zip" "$plugin_url"
					jar xf "${plugin_name}.zip"  # using jar instead of unzip as the latter command is unavailable
					rm "${plugin_name}.zip"
					if [ ! -f manifest.json ]; then  # handle the case zip content is placed within a top-level folder
						dir="$(ls -d ./*/)"
						mv "$dir" __temp
						mv __temp/* .
						rm -Rf __temp
					fi
				)
			else
				echo "INIT: installing $plugin_name from Confluent Hub"
				confluent-hub install --no-prompt "$plugin_name"
			fi
		done
	}
fi

# Abort if just installing plugins
[ "$1" != "--install-only" ] || exit 0

# At first run, deploy connectors in '.json' files, using a concurrent function waiting for Kafka Connect to have started
if [ ! -f /usr/share/confluent-hub-components/connectors-deployed ]; then
	touch /usr/share/confluent-hub-components/connectors-deployed
	function expand_environment_variables() {
		python -c 'import os,sys; sys.stdout.write(os.path.expandvars(sys.stdin.read()))'  # envsubst missing, use python
	}
	function wait_started_and_deploy_connectors() {
		while : ; do
			status="$( curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors )"
			[ "$status" -eq 200 ] && break
			sleep 1
		done
		echo "INIT: processing connectors"
		for file in *.json; do
			name="${file%.json}"
			expand_environment_variables < "$file" | grep -vE '^\s*(#|//).*' > /tmp/connector.json  # expand variables, drop comments
			echo "INIT: deploying $name:";
			cat /tmp/connector.json
			curl -s -X PUT -H "Content-Type:application/json" "http://localhost:8083/connectors/$name/config" -T /tmp/connector.json
			rm /tmp/connector.json
			echo
		done
	}
	wait_started_and_deploy_connectors &
fi

# Run the original command to start Kafka Connect (CMD directive)
exec /etc/confluent/docker/run

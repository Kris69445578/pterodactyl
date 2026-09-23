#!/bin/bash
# =============================================================================
# Pterodactyl Panel Auto-Installer for Ubuntu 22.04 (2GB RAM VPS)
# =============================================================================
# EDIT THE VARIABLES BELOW, then run:
#   chmod +x install-pterodactyl.sh
#   sudo ./install-pterodactyl.sh
# =============================================================================

set -e  # stop on first error

# ---------------------------------------------------------------------------
# >>> EDIT THESE BEFORE RUNNING <<<
# ---------------------------------------------------------------------------
FQDN="bot.jahim.dpdns.org"      # your domain, already pointed at this VPS
EMAIL="benjaminscott0118@gmail.com"       # used for SSL cert + admin account
ADMIN_USERNAME="mseemzimaq"
ADMIN_FIRSTNAME="TONY"
ADMIN_LASTNAME="KINGS"
ADMIN_PASSWORD="klikekliked2"     # change this
TIMEZONE="Africa/Nairobi"
# ---------------------------------------------------------------------------

# Auto-generated (leave these as-is)
DB_PASSWORD=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 24)

echo "===================================================================="
echo " Pterodactyl install starting for $FQDN"
echo "===================================================================="
sleep 2

# ---------------------------------------------------------------------------
# 0. Swap (critical on 2GB RAM)
# ---------------------------------------------------------------------------
if [ ! -f /swapfile ]; then
  echo ">> Creating 2GB swap file"
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# ---------------------------------------------------------------------------
# 1. Base packages + PHP 8.3 repo
# ---------------------------------------------------------------------------
echo ">> Updating system and adding repos"
apt update -y && apt upgrade -y
apt install -y software-properties-common curl apt-transport-https ca-certificates \
  gnupg lsb-release unzip tar git ufw cron

LC_ALL=C.UTF-8 add-apt-repository -y ppa:ondrej/php
apt update -y

echo ">> Installing PHP, MariaDB, Nginx, Redis"
apt install -y php8.3 php8.3-{common,cli,gd,mysql,mbstring,bcmath,xml,fpm,curl,zip,intl,redis} \
  mariadb-server nginx redis-server certbot python3-certbot-nginx

echo ">> Installing Composer"
curl -sS https://getcomposer.org/installer | php
mv composer.phar /usr/local/bin/composer

# ---------------------------------------------------------------------------
# 2. Firewall
# ---------------------------------------------------------------------------
echo ">> Configuring firewall"
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 8080/tcp
ufw allow 2022/tcp
ufw --force enable

# ---------------------------------------------------------------------------
# 3. MariaDB: secure + create panel database
# ---------------------------------------------------------------------------
echo ">> Configuring MariaDB"
# Note: modern MariaDB (10.4+) turns mysql.user into a view and drops the
# legacy Password column, so we don't touch the root account at all here —
# root auths via unix_socket locally, which is fine for this script's needs.
mysql -u root <<EOF
CREATE DATABASE IF NOT EXISTS panel;
CREATE USER IF NOT EXISTS 'pterodactyl'@'127.0.0.1' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON panel.* TO 'pterodactyl'@'127.0.0.1' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EOF

# ---------------------------------------------------------------------------
# 4. Download Panel
# ---------------------------------------------------------------------------
echo ">> Downloading Pterodactyl Panel"
mkdir -p /var/www/pterodactyl
cd /var/www/pterodactyl
curl -Lo panel.tar.gz https://github.com/pterodactyl/panel/releases/latest/download/panel.tar.gz
tar -xzvf panel.tar.gz
chmod -R 755 storage/* bootstrap/cache/

cp .env.example .env
composer install --no-dev --optimize-autoloader --no-interaction

# ---------------------------------------------------------------------------
# 5. Environment + database setup (non-interactive)
# ---------------------------------------------------------------------------
echo ">> Running Panel environment setup"
php artisan key:generate --force

php artisan p:environment:setup \
  --author="${EMAIL}" \
  --url="https://${FQDN}" \
  --timezone="${TIMEZONE}" \
  --cache="redis" \
  --session="redis" \
  --queue="redis" \
  --redis-host="127.0.0.1" \
  --redis-pass="null" \
  --redis-port="6379" \
  --settings-ui=true \
  -n

php artisan p:environment:database \
  --host="127.0.0.1" \
  --port="3306" \
  --database="panel" \
  --username="pterodactyl" \
  --password="${DB_PASSWORD}" \
  -n

echo ">> Running migrations"
php artisan migrate --seed --force

echo ">> Creating admin user"
php artisan p:user:make \
  --email="${EMAIL}" \
  --username="${ADMIN_USERNAME}" \
  --name-first="${ADMIN_FIRSTNAME}" \
  --name-last="${ADMIN_LASTNAME}" \
  --password="${ADMIN_PASSWORD}" \
  --admin=1 \
  -n

# ---------------------------------------------------------------------------
# 6. Permissions
# ---------------------------------------------------------------------------
chown -R www-data:www-data /var/www/pterodactyl/*

# ---------------------------------------------------------------------------
# 7. Cron for queue/scheduler
# ---------------------------------------------------------------------------
echo ">> Setting up cron"
(crontab -l 2>/dev/null; echo "* * * * * php /var/www/pterodactyl/artisan schedule:run >> /dev/null 2>&1") | crontab -

# ---------------------------------------------------------------------------
# 8. Queue worker systemd service
# ---------------------------------------------------------------------------
echo ">> Setting up pteroq queue worker service"
cat > /etc/systemd/system/pteroq.service <<'EOF'
[Unit]
Description=Pterodactyl Queue Worker
After=redis-server.service

[Service]
User=www-data
Group=www-data
Restart=always
ExecStart=/usr/bin/php /var/www/pterodactyl/artisan queue:work --queue=high,standard,low --sleep=3 --tries=3
StartLimitInterval=180
StartLimitBurst=30
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now redis-server
systemctl enable --now pteroq.service

# ---------------------------------------------------------------------------
# 9. Nginx site config (HTTP first, certbot upgrades to HTTPS)
# ---------------------------------------------------------------------------
echo ">> Configuring Nginx"
rm -f /etc/nginx/sites-enabled/default

PHP_SOCK=$(find /run/php -name "*.sock" | head -n1)

cat > /etc/nginx/sites-available/pterodactyl.conf <<EOF
server {
    listen 80;
    server_name ${FQDN};

    root /var/www/pterodactyl/public;
    index index.php;

    access_log /var/log/nginx/pterodactyl.app-access.log;
    error_log  /var/log/nginx/pterodactyl.app-error.log error;

    client_max_body_size 100m;
    client_body_timeout 120s;

    location / {
        try_files \$uri \$uri/ /index.php?\$query_string;
    }

    location ~ \.php\$ {
        fastcgi_split_path_info ^(.+\.php)(/.+)\$;
        fastcgi_pass unix:${PHP_SOCK};
        fastcgi_index index.php;
        include fastcgi_params;
        fastcgi_param PHP_VALUE "upload_max_filesize = 100M \n post_max_size=100M";
        fastcgi_param SCRIPT_FILENAME \$document_root\$fastcgi_script_name;
        fastcgi_param HTTP_PROXY "";
        fastcgi_intercept_errors off;
        fastcgi_buffer_size 16k;
        fastcgi_buffers 4 16k;
        fastcgi_connect_timeout 300;
        fastcgi_send_timeout 300;
        fastcgi_read_timeout 300;
    }

    location ~ /\.ht {
        deny all;
    }
}
EOF

ln -sf /etc/nginx/sites-available/pterodactyl.conf /etc/nginx/sites-enabled/pterodactyl.conf
nginx -t && systemctl restart nginx

# ---------------------------------------------------------------------------
# 10. SSL via Certbot
# ---------------------------------------------------------------------------
echo ">> Requesting SSL certificate for ${FQDN}"
certbot --nginx -d "${FQDN}" --non-interactive --agree-tos -m "${EMAIL}" --redirect || \
  echo "!! Certbot failed — check that ${FQDN} resolves to this VPS's public IP, then run: certbot --nginx -d ${FQDN}"

# ---------------------------------------------------------------------------
# 11. Docker + Wings (the daemon that runs your bot containers)
# ---------------------------------------------------------------------------
echo ">> Installing Docker"
curl -sSL https://get.docker.com/ | CHANNEL=stable bash
systemctl enable --now docker

echo ">> Installing Wings"
mkdir -p /etc/pterodactyl
curl -L -o /usr/local/bin/wings "https://github.com/pterodactyl/wings/releases/latest/download/wings_linux_amd64"
chmod u+x /usr/local/bin/wings

cat > /etc/systemd/system/wings.service <<'EOF'
[Unit]
Description=Pterodactyl Wings Daemon
After=docker.service
Requires=docker.service
PartOf=docker.service

[Service]
User=root
WorkingDirectory=/etc/pterodactyl
LimitNOFILE=4096
PIDFile=/var/run/wings/daemon.pid
ExecStart=/usr/local/bin/wings
Restart=on-failure
StartLimitInterval=180
StartLimitBurst=30
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable wings

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "===================================================================="
echo " PANEL INSTALL COMPLETE"
echo "===================================================================="
echo " URL:            https://${FQDN}"
echo " Admin username: ${ADMIN_USERNAME}"
echo " Admin password: ${ADMIN_PASSWORD}"
echo " DB password:    ${DB_PASSWORD}   (saved to /root/pterodactyl-credentials.txt)"
echo "===================================================================="
echo ""
echo "NEXT STEPS (manual, in the Panel UI):"
echo " 1. Log in at https://${FQDN}"
echo " 2. Admin area -> Locations -> create a location"
echo " 3. Admin area -> Nodes -> create a node (FQDN, memory=1536MB, disk limits)"
echo " 4. Node -> Configuration tab -> copy the config, save it to"
echo "    /etc/pterodactyl/config.yml on this VPS"
echo " 5. Start wings:  systemctl start wings"
echo " 6. Create the custom bot-hosting Egg (I'll build this with you next)"
echo ""

cat > /root/pterodactyl-credentials.txt <<EOF
Panel URL: https://${FQDN}
Admin username: ${ADMIN_USERNAME}
Admin password: ${ADMIN_PASSWORD}
Pterodactyl DB password: ${DB_PASSWORD}
(MySQL root has no password set — it auths via unix_socket locally, which is normal on Ubuntu/MariaDB)
EOF
chmod 600 /root/pterodactyl-credentials.txt

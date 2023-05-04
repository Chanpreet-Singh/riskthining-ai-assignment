publicipaddr=$(dig +short myip.opendns.com @resolver1.opendns.com)
echo "Hope you must have entered "$publicipaddr" instead of api-riskthinking-ai.com in riskthinking-api.conf file, or registered api-riskthinking-ai.com as a domain name"

# Installation of nginx and setting up the service
sudo apt update
sudo apt install nginx
sudo systemctl start nginx
sudo systemctl status nginx

# Connecting nginx service with the API
sudo cp -p riskthinking-api.conf /etc/nginx/sites-available/
sudo chmod 777 /etc/nginx/sites-available/riskthinking-api.conf
sudo ln -sf /etc/nginx/sites-available/riskthinking-api.conf /etc/nginx/sites-enabled/
sudo systemctl restart nginx

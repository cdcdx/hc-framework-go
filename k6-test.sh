clear && export ADMIN_TOKEN=a686d57401b12a5a39478fc584244fbbfd1b6a3276e9104b
make k6-test-auth 
make k6-test-idle && sleep 300
make k6-test-idle-settle && sleep 300
make k6-test-shop && sleep 300
make k6-test-shop-flash && sleep 300
make k6-test-mixed && sleep 300
make k6-test-ws

# AUTH_VUS=150                    k6 run scripts/k6/auth.js
# IDLE_VUS=10000 DBSTRESS_VUS=800 k6 run scripts/k6/idle.js
# MAX_VUS=500                     k6 run scripts/k6/idle_settle.js
# SHOP_VUS=1000                   k6 run scripts/k6/shop.js
# FLASH_VUS=1000                  k6 run scripts/k6/shop_flash.js
# MIXED_VUS=2000                  k6 run scripts/k6/mixed.js
# WS_HEARTBEAT_VUS=10000 WS_RECONNECT_VUS=200 k6 run scripts/k6/ws.js

clear && export ADMIN_TOKEN=a686d57401b12a5a39478fc584244fbbfd1b6a3276e9104b
make k6-test-auth 
make k6-test-idle && sleep 300
make k6-test-idle-settle && sleep 300
make k6-test-shop && sleep 300
make k6-test-shop-flash && sleep 300
make k6-test-mixed



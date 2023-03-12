How to run docker file?

Be in the root directory of project that is inside Upwork 
Execute `docker build -t my-flask-app .`
Then execute `docker run -p 8000:5000 my-flask-app`

http://<ip>:8000 should work in your browser as mentioned in terminal output
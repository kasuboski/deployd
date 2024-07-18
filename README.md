## Running
Create a file in the working directory called `server.json` as below. deployd will then make sure there is a container running like that and load balance between versions.

It currently expects a single `service` and maps `8080` to the port specified.

```json
{
  "name": "test",
  "port": 8080,
  "image": "nginx:alpine"
}
```

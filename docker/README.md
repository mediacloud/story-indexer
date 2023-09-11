## Docker Swarm Deployment

### Prerequisites

Before you begin, ensure you have the following prerequisites in place:

1. Docker: Docker must be installed on the host where you plan to set up the Swarm. You can download and install Docker from Docker's official website.

2. Docker Compose: Make sure you have Docker Compose installed, as it's essential for managing multi-container applications. You can install Docker Compose by following the instructions in the official documentation.

### Swarm setup

To create a swarm

    `docker swarm init`

To deploy in multiple workers/nodes. Join the swarm using the command

    `docker swarm join \ --token <swarm token>`

From the project's root directory

1. Build the images

        `docker compose -f docker/docker-compose.yml build`

2. Push images (optional)

        `docker compose -f docker/docker-compose.yml push`

3. Deploy the swarm stack

        `docker stack deploy -c docker/docker-compose.yml <NAME>`

4. Check service health

To check the health and logs of specific service within the Swarm

        `docker service logs <NAME>_process_name`

5. Remove the stack

        `docker stack rm <NAME>`

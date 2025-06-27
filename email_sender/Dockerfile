FROM node:20-bullseye

WORKDIR /usr/src/app

COPY package*.json ./

RUN chown -R node:node /usr/src/app

USER node

RUN npm install

COPY --chown=node:node . .

EXPOSE 5000

CMD ["npm", "start"]

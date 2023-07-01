'use strict'
const MongoWrapper = require('./wrapper')
const mongo = new MongoWrapper({
  url: 'mongodb://'+process.env.MONGO_USER+':'+process.env.MONGO_PASS+'@'+process.env.MONGO_HOST+'/',
  authDb: process.env.MONGO_AUTH_DB,
  appDb: process.env.MONGO_DB,
  repSet: process.env.MONGO_REPSET
})
let mongoReady = false
const initMongo = async()=>{
  try{
    let status = await mongo.init();
    if(status === true){
      mongoReady = true
    }else{
      setTimeout(initMongo, 5000)
    }
  }catch(e){
    throw(e);
    console.error(initMongo, 5000)
  }
}
initMongo()
module.exports.mongo = mongo
module.exports.mongoReady = mongoReady

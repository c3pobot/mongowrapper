'use strict'
//add text to test
module.exports =  class MongoWrapper {
  constructor(options) {
    this.MongoClient = require('mongodb').MongoClient;
    this.url = options.url;
    this.appDb = options.appDb;
    this.mongo;
    this.dbo
    this.mongoReady = false
    this.compression = true
    if(options?.compression === false) this.compression = false
    if(options.authDb){
      if(this.connectOptions){
        this.connectOptions += '&authSource='+options.authDb
      }else{
        this.connectOptions = '?authSource='+options.authDb
      }
    }
    if(options.repSet){
      if(this.connectOptions){
        this.connectOptions += '&replicaSet='+options.repSet
      }else{
        this.connectOptions = '?replicaSet='+options.repSet
      }
    }
    if(this.compression){
      if(this.connectOptions){
        this.connectOptions += '&compressors=zlib'
      }else{
        this.connectOptions = '?compressors=zlib'
      }
    }
    if(this.connectOptions) this.url += this.connectOptions
  }
  status(){
    return this.mongoReady
  }
  reportError(e, cmd, collection, matchCondition){
    let msg
    if(e?.name) msg = e.name
    if(!msg) msg = 'MongoError'
    msg += '\n'
    msg += 'Request : '+cmd+'\n'
    if(collection) msg += 'collection\n'
    if(matchCondition) msg += JSON.stringify(matchCondition)+'\n'
    if(e?.code) msg += 'Code : '+e.code+'\n'
    if(e?.message) msg += e.message+'\n'
    if(!e.name) msg += e
    throw(msg)
  }
  async init() {
    try {
      this.mongo = await this.MongoClient.connect(this.url, {
        useNewUrlParser: true,
        useUnifiedTopology: true
      });
      this.dbo = await this.mongo.db(this.appDb)
      this.mongoReady = true
      return (true)
    } catch(e) {
      this.reportError(e, 'init')
      return (false)
    }
  }
  async aggregate (collection, matchCondition, pipeline = []){
    try{
      if(matchCondition) pipeline.unshift({$match: matchCondition})
      return await this.dbo.collection(collection).aggregate(pipeline, { allowDiskUse: true }).toArray()

    }catch(e){
      this.reportError(e, 'aggregate', collection, matchCondition)
    }
  }
  async del (collection, matchCondition) {
    try{
      return await this.dbo.collection(collection).deleteOne(matchCondition)
    }catch(e){
      this.reportError(e, 'del', collection, matchCondition)
    }
  }

  async delMany (collection, matchCondition) {
    try{
      return await this.dbo.collection(collection).deleteMany(matchCondition)
    }catch(e){
      this.reportError(e, 'delMany', collection, matchCondition)
    }
  }

  destroy() {
    this.mongo.close();
  }
  async count (collection, matchCondition){
    try{
      return await this.dbo.collection( collection ).countDocuments(matchCondition)
    }catch(e){
      this.reportError(e, 'count', collection, matchCondition)
    }
  }
  async find (collection, matchCondition, project) {
    try{
      return await this.dbo.collection( collection ).find( matchCondition, {projection: project} ).toArray()
    }catch(e){
      this.reportError(e, 'find', collection, matchCondition)
      return []
    }
  }
  async limit (collection, matchCondition, count = 50, project) {
    try{
      return await this.dbo.collection( collection ).find( matchCondition, { projection: project } ).limit( count ).toArray()
    }catch(e){
      this.reportError(e, 'limit', collection, matchCondition)
      return []
    }
  }
  async skip (collection, matchCondition, limitCount = 50, skipCount = 50, project) {
    try{
      return await this.dbo.collection( collection ).find( matchCondition, { projection: project } ).limit( limitCount ).skip( skipCount ).toArray()
    }catch(e){
      this.reportError(e, 'skip', collection, matchCondition)
      return []
    }
  }
  async set (collection, matchCondition, obj, setTTL = true) {
    try{
      if(setTTL && !obj.TTL) obj.TTL = new Date()
      await this.dbo.collection(collection).updateOne(matchCondition,{$set: obj },{"upsert":true})
      delete obj.TTL
      return
    }catch(e){
      this.reportError(e, 'set', collection, matchCondition)
    }
  }
  async setMany (collection, matchCondition, obj, setTTL = true){
    try{
      if(setTTL && !obj.TTL) obj.TTL = new Date()
      await this.dbo.collection(collection).updateMany(matchCondition, {$set: obj}, {upsert: true})
      delete obj.TTL
      return
    }catch(e){
      this.reportError(e, 'setMany', collection, matchCondition)
    }
  }
  async math (collection, matchCondition, obj) {
    try{
      return await this.dbo.collection(collection).updateOne(matchCondition, {$inc: obj, $set: {TTL: new Date()}}, {"upsert":true})
    }catch(e){
      this.reportError(e, 'math', collection, matchCondition)
    }
  }
  async push (collection, matchCondition, obj) {
    try{
      return await this.dbo.collection(collection).updateOne(matchCondition, {$push: obj, $set: {TTL: new Date()}}, {"upsert":true})
    }catch(e){
      this.reportError(e, 'push', collection, matchCondition)
    }
  }
  async pull (collection, matchCondition, obj) {
    try{
      return await this.dbo.collection(collection).updateOne(matchCondition, {$pull: obj, $set: {TTL: new Date()}})
    }catch(e){
      this.reportError(e, 'pull', collection, matchCondition)
    }
  }
  async unset (collection, matchCondition, obj) {
    try{
      return await this.dbo.collection(collection).updateOne(matchCondition, {$unset: obj, $set: {TTL: new Date()}})
    }catch(e){
      this.reportError(e, 'unset', collection, matchCondition)
    }
  }
  async rep(collection, matchCondition, obj) {
    try {
      obj.TTL = new Date()
      await this.dbo.collection(collection).replaceOne(matchCondition, obj, { upsert: true });
      delete obj.TTL
      return
    } catch (e) {
      this.reportError(e, 'rep', collection, matchCondition)
    }
  }
  async next (collection, matchCondition, key){
    try{
      const checkCounter = await this.dbo.collection(collection).findOneAndUpdate(matchCondition,{$inc:{[key]:1}},{returnNewDocument:true, upsert: true})
      if(checkCounter.value){
        return checkCounter.value[key]
      }else{
        const nextValue = await this.dbo.collection(collection).findOneAndUpdate(matchCondition,{$inc:{[key]:1}},{returnNewDocument:true})
        return nextValue.value[key]
      }
    }catch(e){
      this.reportError(e, 'next', collection, matchCondition)
    }
  }
};

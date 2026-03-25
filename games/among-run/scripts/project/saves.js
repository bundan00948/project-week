export default class {
	constructor(){
		console.log("Saver was init...");
		this.KEY = "AmongRunGame.";
	}

	async  getStorageData(key, defaultKey){

		let datas = await rn.storage.getItem(this.KEY + key);
		// If key is empty then make it as 0
		if (datas === null)
		{
			await rn.storage.setItem(this.KEY + key, defaultKey);
			datas = defaultKey;
		}

		return await Promise.resolve(datas);
	}

	async  setStorageData(key, val){
		await rn.storage.setItem(this.KEY + key, val);
	}

}


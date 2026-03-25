export default class {

	constructor(){
		console.log("Shop class was created");
	}
	
	
	checkToOpens(){		
		for(let i = 0; i < 7; i++){
			if(playerProgress.animations[i] == 1){
				rn.callFunction("makeOpen", i);
			}				
		}		
		
		rn.callFunction("moveSelecter", playerProgress.activeAnimation);
	}
	
	
	tryBuyIt(num, cost){
		
		
		if(parseInt(playerProgress.coins) >= cost){
			playerProgress.activeAnimation = num;
			playerProgress.animations[num] = 1;
			rn.callFunction("openAnimation", num);
			playerProgress.coins -= cost;
			cSaves.setStorageData("progress", JSON.stringify(playerProgress));
			
			for(const o of rn.objects.Text.instances())
				if(o.instVars["type"] == "coins")
					o.text = playerProgress.coins.toString();
		}
	}
	
	selectNew(num){
		if(parseInt(playerProgress.animations[num]) == 1){
			playerProgress.activeAnimation = num;
			cSaves.setStorageData("progress", JSON.stringify(playerProgress));
			rn.callFunction("moveSelecter", num);
		}		
	}
	
}
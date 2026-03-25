
// Import any other script files here, e.g.:
// import * as myModule from "./mymodule.js";
import Saves from "./saves.js";
import Shop from "./shop.js";


globalThis.rn = null;
let JSONgame = null;
let lastX = 0;

let player = null;
let lastPlayerX = null;

let score = 0;
let coins = 0;

let shop = null;

globalThis.cSaves = null;
globalThis.playerProgress = null;

runOnStartup(async runtime =>
{
	// Code to run on the loading screen.
	// Note layouts, objects etc. are not yet available.
	
	runtime.addEventListener("beforeprojectstart", () => OnBeforeProjectStart(runtime));
});

async function OnBeforeProjectStart(runtime)
{
	// Code to run just before 'On start of layout' on
	// the first layout. Loading has finished and initial
	// instances are created and available to use here.
	rn = runtime;
	const textFileUrl = await runtime.assets.getProjectFileUrl("games.json");

	const response = await fetch(textFileUrl);
	const fetchedText = await response.json();
	JSONgame = fetchedText;	
	
	if(cSaves == null)
		cSaves = new Saves();	
		
	checkToGold();	
	shop = new Shop();	
	
	
	
	runtime.addEventListener("tick", () => Tick(runtime));
}

function Tick(runtime)
{
	// Code to run every tick
}

function gameInit(){
	player = rn.objects.Player.getAllInstances()[0];
	lastPlayerX = player.x;
	score = 0;
	lastX = 0;
	checkToGold();
	
	if(rn.objects.Player.getFirstPickedInstance() != null)
		rn.objects.Player.getFirstPickedInstance().setAnimation("Animation " + playerProgress.activeAnimation);


}

function gameOver(){
	cSaves.setStorageData("progress", JSON.stringify(playerProgress));
}

function checkToGold(){

	const defaultProgress = {
		coins: 0,
		best: 0,
		activeAnimation: "Animation 1",
		animations: [1,1,0,0,0,0,0,0],
		aLevel : 0,
		aLevels: [5000, 10000, 20000, 30000, 50000, 100000, 9999999]
	}
	
	cSaves.getStorageData("progress", JSON.stringify(defaultProgress)).then(
		function(value){
			playerProgress = JSON.parse(value);
			playerProgress.coins += coins;
			
			coins = 0;
			
			if(score > playerProgress.best)
				playerProgress.best = score;
			updateTexts();
			gameOver();
			updatePlayersDistance();
		}
	);
}


function updatePlayersDistance(){
	let firstPoint = 0;
	let secondPoint = 0;
	let aIndex = 0;
	
	for(let i = 0; i < playerProgress.aLevels.length; i++){
		if(playerProgress.best > playerProgress.aLevels[i])
			firstPoint = playerProgress.aLevels[i];
		if(playerProgress.best < playerProgress.aLevels[i]){
			aIndex = i;
			secondPoint = playerProgress.aLevels[i];
			break;
		}
	}
	
	if(rn.objects.sArrow.getFirstPickedInstance() != null)
		rn.objects.sArrow.getFirstPickedInstance().y = (672 - (96 * (aIndex))) - 96 * (playerProgress.best/secondPoint);
	
}

function checkDistance(){
	lastPlayerX += rn.dt * 2.1;
	if(lastPlayerX > 3){
		generateObstacle(-1); //19
		lastPlayerX = 0;
	}
}

function generateObstacle(num){
	if(num == -1)
		num =  randomz(0, JSONgame.obstacles.length);
		
		
	for(let i = 0; i < JSONgame.obstacles[num][0].length; i++)
		for(let j = 0; j < JSONgame.obstacles[num].length; j++){
			if(JSONgame.obstacles[num][j][i].length == 2){
						let cOb = rn.objects.sFloor.createInstance("game",
												lastX + i * 32, 150 + j * 32);
							cOb.width = JSONgame.obstacles[num][j][i][1] * 32;												
												
			}
			if(JSONgame.obstacles[num][j][i] == "*"){
						rn.objects.sCoin.createInstance("game",
												lastX + 200 + i * 32, 150 + j * 32);
			}
			if(JSONgame.obstacles[num][j][i] == 2){
						rn.objects.sSpark.createInstance("game",
												lastX +200 + i * 32, 150 + j * 32);
			}		
			if(JSONgame.obstacles[num][j][i] == "@"){
						rn.objects.sSaw.createInstance("game",
												lastX +200 + i * 32, 150 + j * 32);
			}
			if(JSONgame.obstacles[num][j][i] == "_"){
						rn.objects.sPoison.createInstance("game",
												lastX +170 + i * 32, 180 + j * 32);
			}
			if(JSONgame.obstacles[num][j][i] == "E"){
						rn.objects.enemy.createInstance("game",
												lastX +200 + i * 32, 150 + j * 32);
			}	
			if(JSONgame.obstacles[num][j][i] == "P"){
						rn.objects.sTopPalka.createInstance("game",
												lastX +200 + i * 32, 50 + j * 32);
			}			
		}
		
		lastX += 30 * 32 + 4;
}

function updateScore(){
	score += 1;
	setInText("score", score);
}

function updateCoins(){
	coins += 1;
	setInText("coins", playerProgress.coins + coins);
}

function updateTexts(){
	setInText("coins", playerProgress.coins);
	setInText("score", score);
	setInText("best", playerProgress.best);
}

function setInText(types, texts){
		for(const o of rn.objects.Text.instances())
			if(o.instVars["type"] == types)
				o.text = texts.toString();
}

function randomz(min, max){
	 return Math.floor(Math.random() * (max - min) ) + min;
}


const scriptsInEvents = {

		async Mainevents_ev_Event1_Act1(runtime, localVars)
		{
			gameInit();
			generateObstacle(0);
		},

		async Mainevents_ev_Event19_Act4(runtime, localVars)
		{
			checkDistance()
			
		},

		async Mainevents_ev_Event27_Act7(runtime, localVars)
		{
			updateCoins()
		},

		async Mainevents_ev_Event28_Act1(runtime, localVars)
		{
			updateScore()
		},

		async Gameover_ev_Event1_Act1(runtime, localVars)
		{
			checkToGold();
		},

		async Shop_ev_Event1_Act3(runtime, localVars)
		{
			shop.checkToOpens();
			checkToGold();
		},

		async Shop_ev_Event3_Act1(runtime, localVars)
		{
			const thisOb = runtime.objects.bUnlock.getPickedInstances()[0];
			shop.tryBuyIt(thisOb.instVars["Type"], thisOb.instVars["cost"])
		},

		async Shop_ev_Event4_Act1(runtime, localVars)
		{
			const thisOb = runtime.objects.sShopFrame.getPickedInstances()[0];
			shop.selectNew(thisOb.instVars["type"])
		}

};

self.C3.ScriptsInEvents = scriptsInEvents;





const scriptsInEvents = {

	async Egame_Event19_Act1(runtime, localVars)
	{
		var newWindow = window.open(`https://yandex.${runtime.globalVars.domain}/games/developer?name=truelisgames&clid=2670653&utm_source=refferal_program&utm_medium={runtime.globalVars.gamename}`,
		          '_blank');
		if (newWindow ==  null)  alert( runtime.globalVars.error );
		parent.location.reload();
	}

};

self.C3.ScriptsInEvents = scriptsInEvents;


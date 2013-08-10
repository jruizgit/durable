
var r = 500;
var sessions = {};

function scratchPad(url, history) {
	var that = {};

	var postMessage = function(d) {		
		try {
			var messageText = document.getElementById("scratchPad").value;
			var message = JSON.parse(messageText);
			server.post(messageText, function(err) {
				if (err) {
					status.text(err.responseText);
				}
				else {
					status.text("OK");
				}
			});
		}
		catch(ex) {
			status.text(ex);
		}
	}
	
	var patchSession = function(d) {
		try {
			var sessionText = document.getElementById("scratchPad").value;
			var session = JSON.parse(sessionText);
			server.send("PATCH", sessionText, function(err) {
				if (err) {
					status.text(err.responseText);
				}
				else {
					status.text("OK");
				}
			});
		}
		catch(ex) {
			status.text(ex);
		}
	}
	
	var copyRecord = function(d) {		
		var record = history.getSelectedRecord(); 
		pad.text(JSON.stringify(record));
	}
	
	that.setStatus = function(message) {
		status.text(message);
	}
		
	var body = d3.select("body");
	
	body.append("br");
	
	body.append("input")
		.attr("type", "button")
		.attr("id", "post")
		.attr("value", "Post Message")
		.on("click", postMessage);
	
	body.append("input")
		.attr("type", "button")
		.attr("id", "patch")
		.attr("value", "Patch Session")
		.on("click", patchSession);
		
	body.append("input")
		.attr("type", "button")
		.attr("id", "copy")
		.attr("value", "Copy Record")
		.on("click", copyRecord);
	
	body.append("br");	
	
	var pad = body.append("textarea")
		.attr("rows", "5")
		.attr("cols", "60")
		.attr("id", "scratchPad")
		.text("{ \"id\": 1, \"content\": \"hello\" }");
	
	body.append("br");
	
	var status = body.append("small")
		.append("span")
		.attr("id", "status");
	
	url = url.substring(0, url.lastIndexOf("/"));
    	
	var server = d3.xhr(url)
		.header("content-type", "application/json; charset=utf-8");
		
	history.onError(that.setStatus);
	return that;
}

function baseVisual(parent, history) {
	var that = {};
	var selectedTime;
	var colorScale;
	var timeOverlay;
	var axisOverlay;
	var startTime;
	var endTime;
	var updateEvents = [];
	
	var timeFormat = d3.time.format("%Y-%m-%d %H:%M:%S.%L");
	var timeLabel = parent.append("text")
		.attr("class", "time label")
		.attr("text-anchor", "start")
		.attr("y", r + 60)
		.attr("x", 30)
		.on("mouseover", updateTimeSelection);
		
	var axisSymbol = d3.svg.symbol()
		.type("triangle-down")
		.size(50);

	var axis = parent.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(" + (r + 20) + ", 30)");

	axis.append("svg:path")
		.attr("tag", "marker");
	
	var updateAxis = function(newStartTime, newEndTime) {
		if (!selectedTime || newEndTime) {
			selectedTime = newEndTime;
			startTime = newStartTime;
			endTime = newEndTime;
		}
		
		colorScale = d3.scale.linear()
						.domain([0xFF, 0x00])
						.range([startTime.getTime(), endTime.getTime()])
						.clamp(true);
				
		var yScale = d3.time.scale().domain([startTime, endTime]).range([10, r - 10]); 
		var yAxis = d3.svg.axis().scale(yScale).orient("right").ticks(10).tickPadding(6).tickSize(10);
		var currentColor = Math.ceil(colorScale.invert(selectedTime.getTime())) * 0x100 + 0xFF0000;									
		
		timeLabel.text(timeFormat(selectedTime));
		axis.call(yAxis);
		axis.selectAll("path[tag=\"marker\"]")
			.attr("class", "timeMarker")
			.style("fill", "#" + currentColor.toString(16))
			.style("fill-opacity", ".7")
			.attr("transform", function(d) { return "translate(-5, " + yScale(selectedTime) + ") , rotate(-90)"; })
			.attr("d", axisSymbol);		
	}

	var updateTimeSelection = function() {
		var timeScale = d3.scale.linear()
			.domain([startTime.getTime(), endTime.getTime()])
			.range([20, r - 60])
			.clamp(true);
			
		timeOverlay.on("mouseover", mouseover)
			.on("mouseout", mouseout)
			.on("mousemove", mousemove)
			.on("touchmove", mousemove);

		function mouseover() {
			timeLabel.classed("active", true);
		}

		function mouseout() {
			timeLabel.classed("active", false);
		}

		function mousemove() {
			selectedTime = new Date(timeScale.invert(d3.mouse(this)[0]));
			selectedTime = history.setSelectedTime(selectedTime);
			timeLabel.text(timeFormat(selectedTime));		
			updateAxis();
			
			for (var i = 0; i < updateEvents.length; ++i) {
				updateEvents[i]();
			}	
		}
	}
		
	var updateAxisSelection = function() {
		var timeScale = d3.scale.linear()
			.domain([startTime.getTime(), endTime.getTime()])
			.range([30, r + 10])
			.clamp(true);
		
		axisOverlay.on("mousemove", mousemove)
			.on("touchmove", mousemove);

		function mousemove() {
			selectedTime = new Date(timeScale.invert(d3.mouse(this)[1]));
			selectedTime = history.setSelectedTime(selectedTime);
			timeLabel.text(timeFormat(selectedTime));		
			updateAxis();					
			for (var i = 0; i < updateEvents.length; ++i) {
				updateEvents[i]();
			}				
		}
	}

	that.onTimeSelected = function(updateFunc) {
		updateEvents.push(updateFunc);
		return that;
	}
	
	that.addOverlays = function() {
		if (!timeOverlay) {
			timeOverlay = parent.append("rect")
				.attr("fill", "none")
				.attr("pointer-events", "all")
				.attr("cursor", "ew-resize")
				.attr("x", 20)
				.attr("y", r + 20)
				.attr("width", r - 75)
				.attr("height", 45)
				.on("mouseover", updateTimeSelection);
				
			axisOverlay = parent.append("rect")
				.attr("fill", "none")
				.attr("pointer-events", "all")
				.attr("cursor", "ns-resize")
				.attr("x", r)
				.attr("y", 30)
				.attr("width", 30)
				.attr("height", r - 15)
				.on("mouseover", updateAxisSelection);
		}
	}
	
	that.getColor = function(node) {		
		if (node.history && node.history.length) {		
			var currentColor = 0xFFFFFF;

			for (var i = 0; i < node.history.length; ++ i) {
				var currentEntry = node.history[i];												
				if (currentEntry.startTime <= selectedTime) {				
					currentColor = Math.ceil(colorScale.invert(currentEntry.startTime.getTime())) * 0x100 + 0xFF0000;									
				}	
				else {
					break;
				}
			}
					
			return "#" + currentColor.toString(16);
		}
		
		return "#fff";
	}

	that.getOpacity = function(node) { 
		if (node.history && node.history.length) {
			if (node.history[0].startTime <= selectedTime) {
				return ".7";
			}		
		}
		
		return "0";
	}

	that.getTitle = function(node) {	
		if (node.history && node.history.length) {
			var displayEntries = "";
			for (var i = 0; i < node.history.length; ++ i) {
				var currentEntry = node.history[i];						
				if (currentEntry.startTime <= selectedTime) {
					displayEntries = displayEntries + currentEntry.startTime.toLocaleString() + "\n";				
					for (var propertyName in currentEntry) {
						if (propertyName !== "_id" && 
							propertyName !== "id" && 
							propertyName !== "endTime" && 
							propertyName !== "startTime") {						
							displayEntries = displayEntries + "\t" + propertyName + ":\t" + currentEntry[propertyName] + "\n";							
						}
					}
				}				
				else {
					break;
				}
			}		
			
			return displayEntries;
		}
		
		return "";
	}
		
	history.onUpdate(updateAxis);
	return that;
}

function stateVisual(root, links, x, y, title, parent, history, base) {
	var base = base || baseVisual(parent, history);
	var that = {};
	var selectedLink = null;
	var zoomedLink = null;
	var pSize = 150;
	var tTime = 0;
	var k = x(r) - x(0);
	k = k / r;
		
	var n = d3.layout.pack()
		.size([r, r])
		.padding(100)
		.value(function(d) { return d.size; })
		.nodes(root);
	
	n.forEach(function(d) { d.y = d.y + 25; });
	
	parent.append("svg:defs").append("svg:marker")
		.attr("id", "end-arrow")
		.attr("viewBox", "0 -5 10 10")
		.attr("refX", 6)
		.attr("markerWidth", 6)
		.attr("markerHeight", 6)
		.attr("orient", "auto")
	.append("svg:path")
		.attr("d", "M0,-5L10,0L0,5")
		.attr("fill", "#ccc");

	var circle = parent.append("svg:g")
		.selectAll("g")
		.data(n);

	var path = parent.append("svg:g")
		.selectAll("path")
		.data(links);
	
	var pathText = parent.append("svg:g")
		.selectAll("text")
		.data(links);
	
	if (title) {
		var titleText =	parent.append("svg:text")
			.attr("class", "time label")
			.attr("x", function(d) { return x(30); })
			.attr("y", function(d) { return y(45); })
			.style("font-size", 40 * k + "px")
			.text(title);
	}
	
	var popup = parent.append("svg:g")
		.selectAll("g")
		.data(links);
	
	that.update = function(transitionTime) { 
		tTime = transitionTime || 0;
		
		path.transition()
			.duration(transitionTime)
			.attr("d", drawLink);

		circle.selectAll("circle").transition()
			.duration(tTime)
			.attr("cx", function(d) { return x(d.x); })
			.attr("cy", function(d) { return y(d.y); })
			.attr("r", function(d) { return k * d.r; })
			.style("fill", base.getColor)
			.style("fill-opacity", base.getOpacity);
			
		circle.selectAll("title")
			.text(base.getTitle);
		
		circle.selectAll("text")
			.transition()
			.duration(tTime)		
			.attr("x", function(d) { return x(d.x); })
			.attr("y", function(d) { return y(d.y); })
			.style("font-size", function(d) { return 12 * k + "px"; });				   
		
		popup.selectAll("rect[tag=\"smPopupBack\"]")
			.transition()
			.duration(tTime)
			.attr("x", function(d) { return x(d.$refX) + 4; })
			.attr("y", function(d) { return y(d.$refY) + 4; })		
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function(d) { return ((d === selectedLink) ? pSize * k: 0)})
			.attr("width", function(d) { return ((d === selectedLink) ? pSize * k: 0)});
		
		popup.selectAll("rect[tag=\"smPopup\"]")
			.transition()
			.duration(tTime)
			.attr("x", function(d) { return x(d.$refX); })
			.attr("y", function(d) { return y(d.$refY); })					
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function(d) { return ((d === selectedLink) ? pSize * k: 0)})
			.attr("width", function(d) { return ((d === selectedLink) ? pSize * k: 0)});
		
		popup.each(updateTransition);
							
	}

	var updateSelection = function(d) {
		popup.style("opacity", function(d1) {
			if (d1 === d) {
				if (selectedLink === d) {
					selectedLink = null;
					return "0";
				}
				else {
					selectedLink = d;
					return "1";
				}
			}
			return "0";
		});	
		
		path.classed("selected", function(d) { return d === selectedLink; })
				
		popup.selectAll("rect[tag=\"smPopupBack\"]")
			.attr("x", function(d) { return x(d.$refX) + 4; })
			.attr("y", function(d) { return y(d.$refY) + 4; })		
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function(d) { return ((d === selectedLink) ? pSize * k: 0)})
			.attr("width", function(d) { return ((d === selectedLink) ? pSize * k: 0)});
		
		popup.selectAll("rect[tag=\"smPopup\"]")
			.attr("x", function(d) { return x(d.$refX); })
			.attr("y", function(d) { return y(d.$refY); })					
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function(d) { return ((d === selectedLink) ? pSize * k: 0)})
			.attr("width", function(d) { return ((d === selectedLink) ? pSize * k: 0)});
		
		d3.event.stopPropagation();
	}
	
	var drawLink = function(d) {
		var deltaX = Math.abs(d.target.x - d.source.x),
		deltaY = Math.abs(d.target.y - d.source.y),
		dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
		angle = Math.atan(deltaY / deltaX),
		refX = Math.cos(angle) * dist / 2 * ( d.source.x > d.target.x ? -1 : 1) + Math.sin(angle) * dist / 8 * ( d.source.y < d.target.y ? -1 : 1) + d.source.x,
		refY = Math.sin(angle) * dist / 2 * ( d.source.y > d.target.y ?-1 : 1) + Math.cos(angle) * dist / 8 * ( d.source.x > d.target.x ? -1 : 1) + d.source.y,
		deltaSourceX = Math.abs(refX - d.source.x),
		deltaSourceY = Math.abs(refY - d.source.y),
		sourceAngle = Math.atan(Math.abs(deltaSourceY / deltaSourceX)),
		deltaTargetX = Math.abs(refX - d.target.x),
		deltaTargetY = Math.abs(refY - d.target.y),
		targetAngle = Math.atan(Math.abs(deltaTargetY / deltaTargetX)),
		sourceR = d.left ? d.source.r : d.source.r ,
		targetR = d.right ? d.target.r + 5 : d.target.r + 5,
		sourceX = d.source.x + Math.cos(sourceAngle) * sourceR * ( d.source.x === d.target.x ? (d.source.y > d.target.y ? -1 : 1) : d.source.x > d.target.x ? -1 : 1),
		sourceY = d.source.y + Math.sin(sourceAngle) * sourceR * ( d.source.y === d.target.y ? (d.source.x > d.target.x ? -1 : 1) : d.source.y > d.target.y ? -1 : 1),
		targetX = d.target.x + Math.cos(targetAngle) * targetR * ( d.source.x === d.target.x ? (d.source.y > d.target.y ? -1 : 1) : d.source.x > d.target.x ? 1 : -1),
		targetY = d.target.y + Math.sin(targetAngle) * targetR * ( d.source.y === d.target.y ? (d.source.x > d.target.x ? -1 : 1) : d.source.y > d.target.y ? 1 : -1);
		d.$startX = sourceX;
		d.$startY = sourceY;
		d.$refX = refX;
		d.$refY = refY;
		return "M" + x(sourceX) + "," + y(sourceY) + "Q" + x(refX) + "," + y(refY) + "," + x(targetX) + "," + y(targetY);
	}
	
	var zoom = function(d) {
		if (zoomedLink !== d) {
			x = d3.scale.linear().range([25, r - 25]);
			y = d3.scale.linear().range([25, r - 25]);
			x.domain([d.$refX, d.$refX + pSize]);
			y.domain([d.$refY, d.$refY + pSize]);
				
			k = (r - 50) / pSize;
			zoomedLink = d;
			that.update(1000);
		}
		else {			
			x = d3.scale.linear().range([0, r]);
			y = d3.scale.linear().range([0, r]);
			x.domain([0, r]);
			y.domain([0, r]);
				
			k = 1;
			zoomedLink = null;
			that.update(1000);
		}
		d3.event.stopPropagation();
	}

	var drawTransition = function(d) {
		if (d.nodes) {				
			var newX = d3.scale.linear().range([x(d.$refX), x(d.$refX + pSize)]);
			var newY = d3.scale.linear().range([y(d.$refY), y(d.$refY + pSize)]);
			newX.domain([0, r]);
			newY.domain([0, r + 50]);
			d.update = sequenceVisual(d.nodes[0], d.links, newX, newY, d.id, d3.select(this), history, base).update;
		}			
	}
	
	var updateTransition = function(d) {
		if (d.nodes) {				
			var newX = d3.scale.linear().range([x(d.$refX), x(d.$refX + pSize)]);
			var newY = d3.scale.linear().range([y(d.$refY), y(d.$refY + pSize)]);
			newX.domain([0, r]);
			newY.domain([0, r + 50]);
			d.update(tTime, newX, newY);
		}			
	}
	
	path.enter()
		.append("svg:path")		
		.attr("class", "link")
		.attr("id", function(d) { return d.source.id + d.id; })
		.style("marker-end", function(d) { return "url(#end-arrow)"; })
		.on("click", updateSelection)
		.attr("d", drawLink);		
	
	pathText.enter()
		.append("svg:text")
		.attr("class", "linkDisplay")		
		.attr("dy", -5)		
		.on("click", updateSelection)
		.append("svg:textPath")		
		.attr("xlink:href", function(d) { return "#" + d.source.id + d.id })
		.attr("startOffset", "5%")
		.text(function(d) { return d.id });
	
	var g = circle.enter().append("svg:g");
	
	g.append("svg:circle")
		.classed("reflexive", function(d) { return d.reflexive; })
		.attr("class", "node")
		.attr("cx", function(d) { return x(d.x); })
		.attr("cy", function(d) { return y(d.y); })
		.attr("r", function(d) { return k * d.r  })
		.style("opacity", function(d) { return (d === root) ? "0" : "1"; });

	g.append("title");
	
	g.append("svg:text")
		.attr("class", "id")
		.attr("text-anchor", "middle")
		.attr("x", function(d) { return x(d.x); })
		.attr("y", function(d) { return y(d.y); })			
		.text(function(d) { return d.id; });	
	
	g = popup.enter()
		.append("svg:g")		
		.style("opacity", "0");
		
	g.append("svg:rect")
		.attr("class", "popupBack")
		.attr("tag", "smPopupBack");
	
	g.append("svg:rect")
		.attr("class", "popup")
		.attr("tag", "smPopup")
		.on("click", zoom);
		
	popup.each(drawTransition);

	base.onTimeSelected(that.update);
	base.addOverlays();
	
	history.onUpdate(function() {
		that.update(500)
	});
	
	d3.select(window).on("click", function() { 
		if (zoomedLink !== null) {
			zoom(zoomedLink);
		}
		else if (selectedLink !== null) {
			updateSelection(selectedLink);
		}
	});

	return that;	
}

function flowVisual(root, links, x, y, title, parent, history, base) { 
	var base = base || baseVisual(parent, history);
	var that = {};
	var selectedStage = null;
	var zoomedStage = null;
	var pSize = 150;
	var tTime = 0;
	var k = x(r) - x(0);
	k = k / r;
	
	var n = d3.layout.tree()
		.size([r, r - 100])
		.nodes(root);
	
	n.forEach(function(d) { d.y = d.y + 90; });

	parent.append("svg:defs").append("svg:marker")
		.attr("id", "end-arrow")
		.attr("viewBox", "0 -5 10 10")
		.attr("refX", 6)
		.attr("markerWidth", 6)
		.attr("markerHeight", 6)
		.attr("orient", "auto")
	.append("svg:path")
		.attr("tag", "arrow")
		.attr("d", "M0,-5L10,0L0,5")
		.attr("fill", "#ccc");
	
	var path = parent.append("svg:g")
		.selectAll("path")
		.data(links);
		  
	var step = parent.append("svg:g")
		.selectAll("g")
		.data(n);
	
	if (title) {
		var titleText =	parent.append("svg:text")
			.attr("class", "time label")
			.attr("x", function(d) { return x(30); })
			.attr("y", function(d) { return y(45); })
			.style("font-size", 40 * k + "px")
			.text(title);
	}
	
	var popup = parent.append("svg:g")
		.selectAll("g")
		.data(n);	
	
	var getSource = function(d) {					
		deltaX = Math.abs(d.target.x - d.source.x),
		deltaY = Math.abs(d.target.y - d.source.y),		
		sourceR = d.source.r / 2 + 2,
		sourceX = d.source.x + sourceR * ((!deltaX && d.source.y > d.target.y) ? 1 : (deltaY > deltaX? 0: (d.source.x > d.target.x ? -1: 1))),
		sourceY = d.source.y + sourceR * ((!deltaX && d.source.y > d.target.y) ? 0 : (deltaY < deltaX? 0: (d.source.y > d.target.y ? -1: 1)));
		return { x: x(sourceX), y: y(sourceY)};		
	}		
	
	var getTarget = function(d) {		
		deltaX = Math.abs(d.target.x - d.source.x),
		deltaY = Math.abs(d.target.y - d.source.y),		
		targetR = d.target.r / 2 + 2,
	    targetX = d.target.x + targetR * ((!deltaX && d.source.y > d.target.y) ? 1 :(deltaY > deltaX? 0: (d.source.x > d.target.x ? 1: -1))),
		targetY = d.target.y + targetR * (deltaY < deltaX? 0: (d.source.y > d.target.y ? 1: -1));
		return { x: x(targetX), y: y(targetY)};		
	}
	
	var diagonal = d3.svg.diagonal()
		.source(getSource)
		.target(getTarget);
		
	that.update = function(transitionTime) { 
		tTime = transitionTime || 0;		
		
		path.transition()
			.duration(tTime)
			.attr("d", diagonal);
	
		step.selectAll("rect").transition()
			.duration(tTime)
			.attr("x", function(d) { return x(d.x - d.r / 2 / ((d.condition) ? Math.sqrt(2) : 1)); })
			.attr("y", function(d) { return y(d.y - d.r / 2 / ((d.condition) ? Math.sqrt(2) : 1)); })
			.attr("height", function(d) { return (d.r / ((d.condition) ? Math.sqrt(2) : 1)) * k; })
			.attr("width", function(d) { return (d.r / ((d.condition) ? Math.sqrt(2) : 1)) * k; })			
			.style("fill", base.getColor)
			.style("fill-opacity", base.getOpacity)
			.attr("transform", function(d) { return (d.condition) ? "rotate(45," + x(d.x) + "," + y(d.y) + ")": ""; });
		
		step.selectAll("title")
			.text(base.getTitle);
			
		step.selectAll("text")
			.transition()
			.duration(tTime)		
			.attr("x", function(d) { return x(d.x); })
			.attr("y", function(d) { return y(d.y + 3); })
			.style("font-size", function(d) { return 12 * k + "px"; });				   
			
		popup.selectAll("rect[tag=\"fcPopup\"]")
			.transition()
			.duration(tTime)
			.attr("x", function(d) { return x(d.x - d.r / 2); })
			.attr("y", function(d) { return y(d.y - d.r / 2); })					
			.attr("height", function(d) { return ((d === selectedStage) ? d.r * k: 0)})
			.attr("width", function(d) { return ((d === selectedStage) ? d.r * k: 0)});
		
		popup.each(updateStage);
	}

	var updateSelection = function(d) {
		popup.style("opacity", function(d1) {
			if (d1 === d) {
				if (selectedStage === d) {
					selectedStage = null;
					return "0";
				}
				else {
					selectedStage = d;
					return "1";
				}
			}
			return "0";
		});	
					
		popup.selectAll("rect[tag=\"fcPopup\"]")
			.attr("x", function(d) { return x(d.x - d.r / 2); })
			.attr("y", function(d) { return y(d.y - d.r / 2); })					
			.attr("height", function(d) { return ((d === selectedStage) ? d.r * k: 0)})
			.attr("width", function(d) { return ((d === selectedStage) ? d.r * k: 0)});
		
		d3.event.stopPropagation();
	}
	
	var zoom = function(d) {
		if (zoomedStage !== d) {
			x = d3.scale.linear().range([25, r - 25]);
			y = d3.scale.linear().range([25, r - 25]);
			x.domain([d.x - d.r / 2 , d.x + d.r / 2]);
			y.domain([d.y - d.r / 2 , d.y + d.r / 2]);
				
			k = (r - 50) / d.r;
			zoomedStage = d;
			that.update(1000);
		}
		else {			
			x = d3.scale.linear().range([0, r]);
			y = d3.scale.linear().range([0, r]);
			x.domain([0, r]);
			y.domain([0, r]);
				
			k = 1;
			zoomedStage = null;
			that.update(1000);
		}
		d3.event.stopPropagation();
	}

	var drawStage = function(d) {		
		if (d.nodes) {				
			var newX = d3.scale.linear().range([x(d.x - d.r / 2), x(d.x + d.r / 2)]);
			var newY = d3.scale.linear().range([y(d.y - d.r / 2), y(d.y + d.r / 2)]);
			newX.domain([0, r]);
			newY.domain([0, r + 50]);
			d.update = sequenceVisual(d.nodes[0], d.links, newX, newY, d.id, d3.select(this), history, base).update;
		}		
	}
	
	var updateStage = function(d) {
		if (d.nodes) {				
			var newX = d3.scale.linear().range([x(d.x - d.r / 2), x(d.x + d.r / 2)]);
			var newY = d3.scale.linear().range([y(d.y - d.r / 2), y(d.y + d.r / 2)]);
			newX.domain([0, r]);
			newY.domain([0, r + 50]);
			d.update(tTime, newX, newY);
		}			
	}
	
	path.enter()
		.append("svg:path")
		.attr("class", "link")
		.style("marker-end", function(d) { return "url(#end-arrow)"; })
		.attr("d", diagonal);

	var g = step.enter().append("svg:g");
	
	g.append("svg:rect")
		.on("click", updateSelection)
		.attr("class", "node")
		.attr("x", function(d) { return x(d.x - d.r / 2 / ((d.condition) ? Math.sqrt(2) : 1)); })
		.attr("y", function(d) { return y(d.y - d.r / 2 / ((d.condition) ? Math.sqrt(2) : 1)); })
		.attr("height", function(d) { return d.r / ((d.condition) ? Math.sqrt(2) : 1); })
		.attr("width", function(d) { return d.r / ((d.condition) ? Math.sqrt(2) : 1); })		
		.attr("transform", function(d) { return (d.condition) ? "rotate(45," + x(d.x) + "," + x(d.y) + ")": ""; });

	g.append("title");
	
	g.append("svg:text")
		.attr("class", "id")
		.attr("text-anchor", "middle")
		.attr("x", function(d) { return x(d.x); })
		.attr("y", function(d) { return y(d.y + 3); })			
		.text(function(d) { return d.id; });	
	
	g = popup.enter()
		.append("svg:g")		
		.style("opacity", "0");
		
	g.append("svg:rect")
		.attr("class", "flowPopup")
		.attr("tag", "fcPopup")
		.on("click", zoom);
	
	popup.each(drawStage);
	
	base.addOverlays();
	base.onTimeSelected(that.update);
	history.onUpdate(function() {
		that.update(500)
	});
	
	d3.select(window).on("click", function() { 
		if (zoomedStage !== null) {
			zoom(zoomedStage);
		}
		else if (selectedStage !== null) {
			updateSelection(selectedStage);
		}
	});
	
	return that;
}

function sequenceVisual(root, links, x, y, title, parent, history, base) { 	
	var base = base || baseVisual(parent, history);
	var that = {};
	var selectedNode = null;
	var k = x(r) - x(0);
	k = k / r;
	var n = d3.layout.tree()
		.size([r - 60, r - 60])
		.nodes(root);
	
	n.forEach(function(d) { (d.top? d.x = r/2: d.x = d.x + 30); d.y = d.y + 65; });
	
	var path = parent.append("svg:g")
		.selectAll("path")
		.data(links);
		  
	var step = parent.append("svg:g")
		.selectAll("g")
		.data(n);

	var promiseSymbol = d3.svg.symbol()
		.type("square")
		.size(90);
	
	var checkpointSymbol = d3.svg.symbol()
		.type("diamond")
		.size(90);
	
	var inputSymbol = d3.svg.symbol()
		.type("triangle-down")
		.size(90);
	
	var tagSymbol = d3.svg.symbol()
		.type("circle")
		.size(90);

	var popup = parent.append("svg:g")
		.selectAll("g")
		.data(n);
	
	if (title) {
		var titleText =	parent.append("svg:text")
			.attr("class", "time label")
			.attr("x", function(d) { return x(30); })
			.attr("y", function(d) { return y(45); })
			.style("font-size", 40 * k + "px")
			.text(title);
	}
	
	var getSource = function(d) {					
		sourceX = d.source.x,
		sourceY = (d.source.y + d.source.r / 2);
		return { x: x(sourceX), y: y(sourceY)};		
	}		
	
	var getTarget = function(d) {		
		targetX = d.target.x;
		targetY = (d.target.y - d.target.r / 2);
		return { x: x(targetX), y: y(targetY)};		
	}
	
	var diagonal = d3.svg.diagonal()
		.source(getSource)
		.target(getTarget);
	
	var drawSymbol = function(d) {
		if (d.type === "tag") {
			return tagSymbol(d);
		}
		
		if (d.type === "receive" || d.type.indexOf("try") === 0) {
			return inputSymbol(d);
		}
		
		if (d.name) {
			return checkpointSymbol(d);
		}
		
		return promiseSymbol(d);
	}
	
	that.update = function(transitionTime, newX, newY) {
		x = newX || x;
		y = newY || y;
		k = x(r) - x(0);
		k = k / r;
		transitionTime = transitionTime || 0;
		
		path.transition()
			.duration(transitionTime)
			.attr("d", diagonal);

		step.selectAll("path")
			.transition()
			.duration(transitionTime)
			.attr("transform", function(d) { return "translate(" + x(d.x) + "," + y(d.y) + "),scale(" + k + ")"; })
			.style("fill", base.getColor)
			.style("fill-opacity", base.getOpacity);
			
		step.selectAll("title")
			.text(base.getTitle);
		
		step.selectAll("text")
			.transition()
			.duration(transitionTime)		
			.attr("x", function(d) { return x(d.x + 10); })
			.attr("y", function(d) { return y(d.y + 3); })
			.style("font-size", function(d) { return 12 * k + "px"; });				   						
					
		popup.selectAll("rect[class=\"popupBack\"]")
			.transition()
			.duration(transitionTime)		
			.attr("x", function(d) { return x(d.x + 14); })
			.attr("y", function(d) { return y(d.y); })		
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function (d) { return ((d === selectedNode) ? d.$height * k: 0)})
			.attr("width", function(d) { return ((d === selectedNode) ? d.$width * k: 0)});
		
		popup.selectAll("rect[class=\"popup\"]")
			.transition()
			.duration(transitionTime)		
			.attr("x", function(d) { return x(d.x + 10); })
			.attr("y", function(d) { return y(d.y - 4); })					
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function (d) { return ((d === selectedNode) ? d.$height * k: 0)})
			.attr("width", function(d) { return ((d === selectedNode) ? d.$width * k: 0)});
	
		popup.selectAll("text")
			.transition()
			.duration(transitionTime)		
			.attr("x", function(d) { return x(d.x + 10); })
			.attr("y", function(d) { return y(d.y + 9); })
			.style("font-size", 12 * k + "px");
			
		if (titleText) {
			titleText.transition()
				.duration(transitionTime)		
				.attr("x", function(d) { return x(30); })
				.attr("y", function(d) { return y(45); })
				.style("font-size", 40 * k + "px");
		}
		
	}
	
	var updateSelection = function(d) {		
		popup.style("opacity", function(d1) {
			if (d1 === d) {
				if (selectedNode === d) {
					selectedNode = null;
					return "0";
				}
				else {
					selectedNode = d;
					return "1";
				}
			}
			return "0";
		});	
		
		popup.selectAll("rect[class=\"popupBack\"]")
			.attr("x", function(d) { return x(d.x + 14); })
			.attr("y", function(d) { return y(d.y); })		
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function (d) { return ((d === selectedNode) ? d.$height * k: 0)})
			.attr("width", function(d) { return ((d === selectedNode) ? d.$width * k: 0)});
		
		popup.selectAll("rect[class=\"popup\"]")
			.attr("x", function(d) { return x(d.x + 10); })
			.attr("y", function(d) { return y(d.y - 4); })					
			.attr("rx", 8 * k)
			.attr("ry", 8 * k)
			.attr("height", function (d) { return ((d === selectedNode) ? d.$height * k: 0)})
			.attr("width", function(d) { return ((d === selectedNode) ? d.$width * k: 0)});
	
		popup.selectAll("text")
			.attr("x", function(d) { return x(d.x + 10); })
			.attr("y", function(d) { return y(d.y + 9); })
			.style("font-size", 12 * k + "px");
		
		if (titleText) {
			titleText.attr("x", function(d) { return x(30); })
				.attr("y", function(d) { return y(45); })
				.style("font-size", 40 * k + "px");
		}
		
		d3.event.stopPropagation();
	}
	
	var setDisplayProperties = function(d) {
		var displayValues = [];
		if (d.params) {
			var maxValue = 0;
			var i = 0;		
			for (var propertyName in d.params) {						
				var currentValue = JSON.stringify(d.params[propertyName]);
				if (currentValue.length > 25) {
					currentValue = currentValue.substring(0, 25) + "...";
				}
				
				if (propertyName.length > 15) {
					propertyName = propertyName.substring(0, 15) + "...";
				}
								
				displayValues.push({x: d.x + 10, y: d.y + (i * 15), value: propertyName + ":", property: true});									
				displayValues.push({x: d.x + 80, y: d.y + (i * 15), value: currentValue});									
				++i;
			}
								
		}		
		
		d.$height = displayValues.length ? displayValues.length/2 * 15 + 4 : 0;
		d.$width = displayValues.length ? 230: 0;			
		d.$displayValues = displayValues;		
	}
	
	path.enter()
		.append("svg:path")
		.attr("class", "link")
		.attr("d", diagonal);

	var g = step.enter().append("svg:g");
	
	g.append("svg:path")
		.attr("class", "sequenceNode")
		.attr("transform", function(d) { return "translate(" + x(d.x) + "," + y(d.y) + "),scale(" + k + ")"; })
		.attr("d", drawSymbol)
		.on("click", updateSelection);		
	
	g.append("title");	
		
	g.append("svg:text")		
		.attr("class", "id")
		.attr("text-anchor", "right")
		.attr("x", function(d) { return x(d.x + 10); })
		.attr("y", function(d) { return y(d.y + 3); })			
		.style("font-size", function(d) { return 12 * k + "px"; })
		.text(function(d) { return (d.name ? d.name : d.type); });	
		
	g = popup.enter()
		.append("svg:g")
		.attr("type", "popup")
		.style("opacity", "0");
	
	popup.each(setDisplayProperties);
	
	g.append("svg:rect")
		.attr("class", "popupBack");
	
	g.append("svg:rect")
		.attr("class", "popup");
	
	g.selectAll("text")
		.data(function(d) { return d.$displayValues })
		.enter()
		.append("svg:text")		
		.attr("class", function(d) { return (d.property ? "popupProperty" : "popupValue"); })
		.text(function(d) { return d.value; });
	
	base.addOverlays();
	base.onTimeSelected(that.update);
	history.onUpdate(function() {
		that.update(500)
	});
	
	d3.select(window).on("click", updateSelection);
	
	return that;
}


function promiseHistory(url) {
	var that = {};
	var records = [];
	var startTime;
	var endTime;
	var updateEvents = [];
	var newRecordEvents = [];
	var errorEvents = [];
	var selectedRecord;
	var promiseUrl = url.substring(0, url.lastIndexOf("/"));
	promiseUrl = promiseUrl.substring(0, promiseUrl.lastIndexOf("/"));
					
	
	that.onUpdate = function(updateFunc) {
		updateEvents.push(updateFunc);
		return that;
	}
	
	that.onNewRecord  = function(newRecordFunc) {
		newRecordEvents.push(newRecordFunc);
		return that;
	}
	
	that.onError  = function(errorFunc) {
		errorEvents.push(errorFunc);
		return that;
	}
		
	that.setSelectedTime = function(time) {
		var top = records.length;		
		var bottom = 0;
		for (;;) {		
			var currentIndex = bottom + Math.floor((top - bottom) / 2);
			selectedRecord = records[currentIndex];
			if (time >= selectedRecord.endTime) {
				bottom = currentIndex;
			}
			else if (time < selectedRecord.startTime) {
				top = currentIndex;
			}
			else {
				break;
			}
			
			if (currentIndex === records.length - 1) {
				break;
			}
		}
	
		return selectedRecord.startTime;
	}
	
	that.getSelectedRecord = function() {		
		var newRecord = {};
		for (var propertyName in selectedRecord) {
			newRecord[propertyName] = selectedRecord[propertyName];
		}
		delete(newRecord._id);
		delete(newRecord.id);
		delete(newRecord.startTime);
		delete(newRecord.endTime);
		delete(newRecord.status);
		
		return newRecord;
	}
	
	var getTimeFromId = function (objectId) {
		var hexTime = objectId.substring(0,8);	
		hexTime = parseInt(hexTime, 16) * 1000;
		return new Date(hexTime);
	}

	var update = function() {   
		for (var sessionName in sessions) {		
			var currentSession = sessions[sessionName];		
			var historyUrl = promiseUrl + "/" + sessionName + "/history?$filter={ \"_id\": { \"$gt\": \"" + currentSession._id + "\"}}";
			d3.json(historyUrl, function(err, history) {
				if (err) {
					for (var i = 0; i < errorEvents.length; ++i) {
						errorEvents[i](err.responseText);
					}									
				}
				else {
					var previousEntry;					
					var currentSessionName;
					for (var i = 0; i < history.length; ++i) {				
						var currentEntry = history[i];	
						records.push(currentEntry);
						if (!currentSessionName) {
							currentSessionName = currentEntry.id;						
						}
							
						currentEntry.startTime = getTimeFromId(currentEntry._id);					
						if (previousEntry) {
							previousEntry.endTime = currentEntry.startTime;
						}
						else {
							var sessionEntry = sessions[currentSessionName];
							sessionEntry.endTime = currentEntry.startTime;
							sessionEntry.status = "checked";					
							if (!startTime || currentEntry.startTime < startTime) {
								startTime = currentEntry.startTime;
							}
						}
														
						for (var ii = 0; ii < newRecordEvents.length; ++ii) {
							newRecordEvents[ii](currentEntry, currentSessionName);
						}
						
						previousEntry = currentEntry;
					}
										
					if (previousEntry) {
						previousEntry.endTime = previousEntry.startTime;				
						sessions[currentSessionName] = previousEntry;
						
						if (!endTime || endTime < previousEntry.startTime) {
							endTime = previousEntry.startTime;				
						}
					}
					
					if (history.length) {
						for (var i = 0; i < updateEvents.length; ++i) {
							updateEvents[i](startTime, endTime);
						}
					}
				}				
			});		
		}	
	}

	var timeout = function() {
		update();
		setTimeout(timeout, 5000);
	}

	that.start = function() {
		timeout();
	}
	
	return that;
}

function promiseGraph(url, history) {
	var that = {};
	var links = [];
	var nodes;
	var nodeDictionary = {};
	var sessionName = url.substring(0, url.lastIndexOf("/"));
	var promiseUrl = sessionName.substring(0, sessionName.lastIndexOf("/"));			
	sessionName = sessionName.substring(sessionName.lastIndexOf("/") + 1);
	
	if (sessionName.indexOf(".") !== -1) {
		sessionName = sessionName.substring(0, sessionName.indexOf("."));
	}
	
	var getIdFromTime = function (timestamp) {
		var newTime = new Date(timestamp).getTime();
		var hexSeconds = Math.floor(newTime / 1000).toString(16);        
		return hexSeconds + "0000000000000000";        
	}
	
	var getStateNodes = function(chart, links, parentId) {    	
		var currentState;
		var resultNode = { size:20, id:"", history: [], children: [] };
		var stateNode;
		var stateId;
		
		for (var stateName in chart) {
			currentState = chart[stateName];
			if (parentId) {
				stateId = parentId + "." + stateName;
			}
			else {
				stateId = stateName;
			}
			
			if (currentState.$chart) {			 
				stateNode = getStateNodes(currentState.$chart, links, stateId);
				stateNode.id = stateId;
			}
			else {
				stateNode = { id: stateId, history: [], size:20 }; 
			}
			
			nodeDictionary[stateId] = stateNode;
			resultNode.children.push(stateNode);						
		}
		
		for (var stateName in chart) {		
			currentState = chart[stateName];
			if (parentId) {
				stateId = parentId + "." + stateName;
			}
			else {
				stateId = stateName;
			}
			
			for (transitionName in currentState) {			
				var transition = currentState[transitionName];
				var transitionNode = { r:14, name: stateId, type:"tag", top : true, children: [] };
				var transitionLinks = [];
				var outputNode;
				var transitionId;
				var toTransitionId; 
				
				if (parentId) {
					transitionId = parentId + "." + transitionName;
				}
				else {
					transitionId = transitionName;
				}
				
				if (transition.to) {
					if (parentId) {
						toTransitionId = parentId + "." + transition.to;
					}
					else {
						toTransitionId = transition.to;
					}
				}
				
				if (transition.whenAny) {
					outputNode = getEventNodes(transitionNode, transition.whenAny, false, transitionLinks);									
				}
				else if (transition.whenAll) {
					outputNode = getEventNodes(transitionNode, transition.whenAll, true, transitionLinks);									
				}
				else if (transition.when) {
					outputNode = { r:14, top: true, children: [] };
					outputNode.type = transition.when.type;				
					outputNode.params = transition.when.params;
					transitionNode.children.push(outputNode);
					transitionLinks.push({ source: transitionNode, target: outputNode, left: false, right: true });			
				}
				else {
					outputNode = transitionNode;
				}

                var toNode;
				if (transition.run) {	
					toNode = { r:14, name: toTransitionId, type:"tag", top: true, children: [] };
					var resultNodes = getSequenceNodes(transition.run, transitionLinks, null, null, outputNode, toNode);
					outputNode = resultNodes[1];
				}
				else if (transition.to) {
				    toNode = { r:14, name: toTransitionId, type:"tag", top: true, children: [] };
					outputNode.children.push(toNode);
					transitionLinks.push({ source: outputNode, target: toNode, left: false, right: true });	
				}
				
				if (transition.to) {			
					links.push({source: nodeDictionary[stateId], 
								target: nodeDictionary[toTransitionId], 
								nodes: [transitionNode, outputNode], 
								links: transitionLinks,
								id: transitionId, 
								left: false, 
								right: true });
				}							
			}		
		}		
		
		resultNode.type = "stateChart";
		return resultNode;
	}

	var getFlowNodes = function(chart, links) {		
		var resultNode;    
		var visitedNodes = {};
		for (var stateName in chart) {		
			var currentState = chart[stateName];
			var stateNode = { id: stateName, r:50 , history: [], children: [] };
			if (currentState.run) {			
				var startNode = { r:14, name: "$start" , type:"tag", top: true, children: [] };
				var endNode = { r:14, name: "$end" , type:"tag", top: true, children: [] };
				stateNode.links = [];
				stateNode.nodes = getSequenceNodes(currentState.run, stateNode.links, null, null, startNode, endNode);			
			}
			
			nodeDictionary[stateName] = stateNode;
			if (!resultNode) {
				resultNode = stateNode;
				visitedNodes[stateName] = stateNode;
			}	
		}
		
		var targetNode;
		var sourceNode;
		var currentState;
		for (var stateName in chart) {		
			currentState = chart[stateName];
			sourceNode = nodeDictionary[stateName];
			if (currentState.to) {
				if (typeof(currentState.to) === "string") {
					targetNode = nodeDictionary[currentState.to];
					if (targetNode) {
						if (!visitedNodes[currentState.to]) {
							sourceNode.children.push(targetNode);
							visitedNodes[currentState.to] = targetNode;
						}
						
						links.push({ source: sourceNode, target: targetNode, left: false, right: true });
					}
				}
				else {
					var conditionNode = { id: "switch", r:50, children: [], condition: true };
					sourceNode.children.push(conditionNode);
					links.push({ source: sourceNode, target: conditionNode, left: false, right: true });
					for (var targetName in currentState.to) {
						targetNode = nodeDictionary[targetName];
						if (targetNode) {
							if (!visitedNodes[targetName]) {
								conditionNode.children.push(targetNode);
								visitedNodes[targetName] = targetNode;
							}
							
							links.push({ source: conditionNode, target: targetNode, left: false, right: true });
						}
					}
				}
			}			
		}	
		
		resultNode.type = "flowChart";
		return resultNode;
	}

	var getParallelNodes = function(streams, all, parentName, promiseName, links) {	
		var rootNode = { r:14, name: promiseName, type: "checkpoint", history: [], children: [] };
		nodeDictionary[promiseName] = rootNode;
		var endNodes = [];
		var endNode = { r:14, name: (all? "all": "any"), type: "receive", children: [] };
		for (var forkName in streams) {		
			var currentPromises = streams[forkName];
			var currentNodes = getSequenceNodes(currentPromises, links, parentName, forkName);
			rootNode.children.push(currentNodes[0]);
			links.push({ source: rootNode, target: currentNodes[0], left: false, right: true });
			endNodes.push(currentNodes[1]);
			links.push({ source: currentNodes[1], target: endNode, left: false, right: true });
		}
					
		endNodes[Math.floor(endNodes.length / 2)].children.push(endNode);
		return [rootNode, endNode];
	}

	var getEventNodes = function(rootNode, events, all, links) {
		var endNode = { r:14, name: (all? "and": "or"), type: "tag", children: [] };
		var endNodes = [];
		for (var eventName in events) {		
			var currentPromise = events[eventName];
			var currentNode;
			if (eventName.indexOf("$and") !== -1) {
				currentNode = getEventNodes(rootNode, currentPromise, true, links);
			}
			else if (eventName.indexOf("$or") !== -1) {
				currentNode = getEventNodes(rootNode, currentPromise, false, links);
			} 
			else {
				currentNode = { r:14, children: [] };
				currentNode.type = currentPromise[0].type;	
				currentNode.depth = 0;
				currentNode.params = currentPromise[0].params;
				rootNode.children.push(currentNode);
				links.push({ source: rootNode, target: currentNode, left: false, right: true });
			}
							
			endNodes.push(currentNode);
			links.push({ source: currentNode, target: endNode, left: false, right: true });
		}
		
		var maxDepthNode;	
		for(var i = 0; i < endNodes.length; ++i) {	
			var currentNode = endNodes[i];
			if (!maxDepthNode) {
				maxDepthNode = currentNode;
			}
			else {
				if (currentNode.depth > maxDepthNode.depth) {
					maxDepthNode = currentNode;
				}
			}
		}
		
		maxDepthNode.children.push(endNode);
		endNode.depth = maxDepthNode.depth + 1;
		return endNode;	
	}

	var getSequenceNodes = function(promises, links, sessionName, forkName, rootNode, endNode) {	
		var top = !(forkName);
		
		if (sessionName) 
		{
			if (forkName) {
				rootNode = { r:14, name: forkName, type:"checkpoint", top: top, history: [], children: [] };
				sessionName = sessionName + "." + forkName;
			}
			else {
				rootNode = { r:14, name: "$start:" + sessionName, type:"tag", top : top, history: [], children: [] };
			}
				
			sessions[sessionName] = { _id: getIdFromTime(new Date(2013, 01, 01, 0, 0, 0)) };	
			endNode = { r:14, name: "$end:" + sessionName, type:"tag", top: top, history: [], children: [] };
		}
		
		var previousNode = rootNode;
		for (var i = promises.length -1; i >= 0; --i) {
			var currentPromise = promises[i];
			if (currentPromise.type === "stateChart") {
				return getStateNodes(currentPromise.params.chart, links);			
			}
			else if (currentPromise.type === "flowChart") {
				return getFlowNodes(currentPromise.params.chart, links);
			}
			else if (currentPromise.type === "waitStreams") {
				var resultNodes = getParallelNodes(currentPromise.params.streams, currentPromise.params.all, sessionName, currentPromise.params.name, links);						
				previousNode.children.push(resultNodes[0]);
				links.push({ source: previousNode, target: resultNodes[0], left: false, right: true });		
				previousNode = resultNodes[1];
				previousNode.top = top;
			}
			else if (currentPromise.type === "waitEvents") {
				var resultNode = getEventNodes(previousNode, currentPromise.params.events, currentPromise.params.all, links);									
				previousNode = resultNode;
				previousNode.top = top;
			}
			else {
				var currentNode = { r:14, top: top, children: [] };
				currentNode.name = currentPromise.params.name;
				currentNode.type = currentPromise.type;
				currentNode.params = currentPromise.params;
				if (currentNode.name) {
					currentNode.history = [];
					nodeDictionary[currentNode.name] = currentNode;
				}
				
				previousNode.children.push(currentNode);
				links.push({ source: previousNode, target: currentNode, left: false, right: true });		
				previousNode = currentNode;
			}						
		}
		
		if (sessionName) {
			nodeDictionary[rootNode.name] = rootNode;
			nodeDictionary[endNode.name] = endNode;
		}
		previousNode.children.push(endNode);
		links.push({ source: previousNode, target: endNode, left: false, right: true });	
		return [rootNode, endNode];
		
	}

	var addHistoryRecord = function(entry, sessionName) {		
		if (entry.status === "complete" && nodeDictionary["$end:" + sessionName]) {			
			nodeDictionary[entry.label].history.push(entry);							
			nodeDictionary["$end:" + sessionName].history.push(entry);								
		}
		else if (!entry.label && nodeDictionary["$start:" + sessionName]) {
			nodeDictionary["$start:" + sessionName].history.push(entry);
		}
		else if (nodeDictionary[entry.label]) {				
			nodeDictionary[entry.label].history.push(entry);							
		}
	}
	
	that.createVisual = function(parent, callback) {
		d3.json(promiseUrl, function(err, promises) {
			if (err) {
				var error;
				try {
					error = JSON.parse(err.responseText);
				}
				catch(ex) {
					error = { error: ex };
				}
				
				callback(error);				
			}
			else {
				nodes = getSequenceNodes(promises, links, sessionName);
				
				var x = d3.scale.linear().range([0, r]);
				var y = d3.scale.linear().range([0, r]);
				var title = promiseUrl.substring(1) + "/" + sessionName;
				x.domain([0, r]);
				y.domain([0, r]);
				
				if (nodes.type === "stateChart") {		
					stateVisual(nodes, links, x, y, title, parent, history);
				}
				else if (nodes.type === "flowChart") {		
					flowVisual(nodes, links, x, y, title, parent, history);
				}
				else {
					sequenceVisual(nodes[0], links, x, y, title, parent, history);
				}				
			
				history.start();				
				if (callback) {
					callback();
				}			
			}
		});	
	}
	
	history.onNewRecord(addHistoryRecord);
	return that;
}


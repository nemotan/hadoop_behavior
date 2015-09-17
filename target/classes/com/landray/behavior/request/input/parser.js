return {
	java : {
		parser : 'JavaParser',
		modules : [ {
			path : '/moduleindex.jsp',
			parser : 'JavaIndexParser'
		}, {
			path : '/moduleindex_notopic.jsp',
			parser : 'JavaIndexParser'
		}, {
			path : '/sys/portal/',
			parser : 'JavaPortalParser'
		}, {
			path : '/sys/ftsearch/searchBuilder.do',
			parser : 'JavaSearchParser'
		} ]
	},
	domino : {
		parser : 'DominoParser'
	}
};
package edu.umass.cics.ciir.dbpedia

/**
 *
 * @author jfoley.
 */
val CategoryPrefix = "http://dbpedia.org/resource/Category:"
val ShortCategoryPrefix = "Category:"
fun getCategory(input: String): String {
    if(input.startsWith(CategoryPrefix)) {
        return input.substring(CategoryPrefix.length)
    }
    if (input.startsWith(ShortCategoryPrefix)) {
        return input.substring(ShortCategoryPrefix.length)
    }
    return input
}
fun categoryOrNull(x: String): String? {
    if (x.startsWith(CategoryPrefix)) {
        return x.substring(CategoryPrefix.length)
    } else return null
}

fun isWikidata(url: String): Boolean = url.startsWith(QPagePrefix)
fun isResource(url: String): Boolean = url.startsWith(PagePrefix)
fun getKey(url: String): String {
    val start = if (isResource(url)) {
        PagePrefix.length
    } else if (isWikidata(url)) {
        QPagePrefix.length
    } else {
        0
    }
    val beforeQueryParams = url.lastIndexOf("?")
    if (beforeQueryParams >= 0) {
        return url.substring(start, beforeQueryParams)
    }
    return url.substring(start)
}
fun lastSlash(url: String): String {
    var beforeQueryParams = url.lastIndexOf("?")
    var lastSlash = url.lastIndexOf('/')
    if (lastSlash < 0) {
        lastSlash = 0
    } else {
        lastSlash += 1
    }
    if (beforeQueryParams < 0) beforeQueryParams = url.length
    return url.substring(lastSlash, beforeQueryParams)
}
fun tokenizeCamelCase(camels: String): List<String> {
    val splits = camels.mapIndexed { i, ch ->
        if (Character.isUpperCase(ch)) {
            i
        } else null
    }.filterNotNull().toMutableList()

    val all_splits = arrayListOf(0)
    all_splits.addAll(splits)
    all_splits.add(camels.length)

    val tolower = camels.toLowerCase()
    return (0 until all_splits.size-1).map { i ->
        val left = all_splits[i]
        val right = all_splits[i+1]
        tolower.substring(left, right)
    }.toList()
}
fun propToText(prop: String) = tokenizeCamelCase(prop).joinToString(separator = " ") { it.trim() }
fun getProp(url: String): String {
    if (url.endsWith("#seeAlso")) {
        return "see also"
    } else {
        return lastSlash(url)
    }
}
fun wikiTitleToText(xyz: String): String {
    val simplified = getCategory(xyz).replace('_', ' ')
    val withoutCamels = simplified.split(" ").map { words ->
        propToText(words.trim())
    }.joinToString(separator = " ") { it.trim() }

    if (simplified.toLowerCase() == withoutCamels) {
        return simplified
    }
    return simplified+" "+withoutCamels
}


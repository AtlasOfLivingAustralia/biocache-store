<xsl:stylesheet xmlns:xsl = "http://www.w3.org/1999/XSL/Transform" xmlns:mt ="http://www.iana.org/assignments" version="2.0">
    <xsl:output method="text" encoding="UTF-8" media-type="text/plain"/>
    <xsl:template match="/">
        <xsl:apply-templates select="//mt:record"/>
    </xsl:template>

    <xsl:template match="mt:record">
        <xsl:choose>
            <xsl:when test="mt:file"><xsl:value-of select="mt:file"/></xsl:when>
            <xsl:otherwise><xsl:value-of select="../@id"/>/<xsl:value-of select="mt:name"/></xsl:otherwise>
        </xsl:choose>
        <xsl:text>&#9;</xsl:text>
        <xsl:value-of select="mt:name"/>
        <xsl:text>
</xsl:text>
    </xsl:template>
</xsl:stylesheet>
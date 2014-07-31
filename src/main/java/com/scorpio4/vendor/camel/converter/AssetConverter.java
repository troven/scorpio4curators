package com.scorpio4.vendor.camel.converter;

import com.scorpio4.assets.Asset;
import org.apache.camel.Converter;

import java.io.IOException;

/**
 * scorpio4-oss (c) 2014
 * Module: com.scorpio4.vendor.camel.converter
 * User  : lee
 * Date  : 31/07/2014
 * Time  : 10:41 PM
 */
@Converter
public class AssetConverter {

	@Converter
	public static String toString(Asset asset) {
		return asset.toString();
	}

	@Converter
	public static java.io.OutputStream toOutputStream(Asset asset) throws IOException {
		return asset.getOutputStream();
	}

}

package com.facebook.campaignstats.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONObject;

import com.facebook.campaignstats.main.App;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;

public class CampaignData {

	private static final String URL = "https://graph.facebook.com";
	private static final String VERSION = "v2.11";
	private static final String SESSION_TOKEN = "EAAWXmQeQZAmcBAJHkodkpmV1ZBX7S9t9eQDsmbBpr0KVFAoGYhV9vAw00a2sYkoM22lVp0s0RVxi0Uiw2cgxjBJyJ3AZAbwEP4MRZAFU7e0kq6ssOtUMMOsgZA2bYMypkyS8HDtJYHb3Wa8fPknN8Vd7cieifdE8j77qisyyw4wZDZD";

	public static int getCampaignStats(String account_id) {

		try {

			String custom_url = URL + "/" + VERSION + "/act_" + account_id
					+ "/insights?level=campaign&fields=impressions,inline_link_clicks,spend,actions,reach,account_id,total_action_value,action_values,clicks,account_currency,account_name,frequency,date_start,date_stop,campaign_id,campaign_name,objective&limit=5000&date_preset=yesterday&action_report_time=conversion&action_attribution_windows=['28d_view','28d_click']&access_token=" + SESSION_TOKEN;

			HttpClient reqclient = new DefaultHttpClient();
			HttpGet reqget = new HttpGet(custom_url);

			System.out.println("Sending GET request : " + custom_url);

			HttpResponse response = reqclient.execute(reqget);

			System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String line = "";

			while ((line = reader.readLine()) != null) {

				buffer.append(line);

			}

			System.out.println("Response Content : " + buffer.toString());

			if (response.getStatusLine().getStatusCode() >= 500) {
				Thread.sleep(5000);
				return 500;
			}

			if (response.getStatusLine().getStatusCode() >= 400 && response.getStatusLine().getStatusCode() < 400) {
				return 400;
			}

			if (response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300) {

				JSONObject responseObj = new JSONObject(buffer.toString());

				if (responseObj.has("data")) {

					JSONArray dataObj = responseObj.getJSONArray("data");

					if (dataObj.length() > 0) {

						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						
						for(int arr_i = 0; arr_i < dataObj.length(); arr_i++) {
	
							JSONObject dataset = dataObj.getJSONObject(arr_i);
	
							Rows row = new Rows();
							HashMap<String, Object> report = new HashMap<String, Object>();
	
							if(dataset.has("campaign_id")) {
								report.put("campaign_id", dataset.getString("campaign_id"));
							}
							
							if(dataset.has("campaign_name")) {
								report.put("campaign_name", dataset.getString("campaign_name"));
							}
							
							if(dataset.has("objective")) {
								report.put("objective", dataset.getString("objective"));
							}
							
							if(dataset.has("account_id")) {
								report.put("account_id", "act_" + dataset.getString("account_id"));
							}
							
							if(dataset.has("account_name")) {
								report.put("account_name", dataset.getString("account_name"));
							}
							
							if(dataset.has("date_start")) {
								report.put("start_date", dataset.getString("date_start"));
							}
							else {
								continue;
							}
							
							if(dataset.has("date_stop")) {
								report.put("end_date", dataset.getString("date_stop"));
							}
							else {
								continue;
							}
							
							report.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()));
	
							if(dataset.has("impressions")) {
								report.put("impressions", dataset.getInt("impressions"));
							}
							
							if(dataset.has("account_currency")) {
								report.put("account_currency", dataset.getString("account_currency"));
							}
							
							if(dataset.has("clicks")) {
								report.put("clicks", dataset.getInt("clicks"));
							}
	
							if(dataset.has("inline_link_clicks")) {
								report.put("inline_link_clicks", dataset.getInt("inline_link_clicks"));
							}
							
							if(dataset.has("spend")) {
								report.put("spend", dataset.getDouble("spend"));
							}
							
							if(dataset.has("frequency")) {
								report.put("frequency", dataset.getDouble("frequency"));
							}
							
							if(dataset.has("reach")) {
								report.put("reach", dataset.getInt("reach"));
							}

							if (dataset.has("action_values")) {
								if (!dataset.isNull("action_values")) {
									JSONArray ar = dataset.getJSONArray("action_values");
									if (ar.length() != 0) {
										for (int j = 0; j < ar.length(); j++) {
											
											if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_purchase")) {
											
												if (ar.getJSONObject(j).has("value")) {
													report.put("oc_fb_pixel_purchase_priceValue",
															ar.getJSONObject(j).getDouble("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("fb_pixel_purchase_28d_click_value",
															ar.getJSONObject(j).getDouble("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("fb_pixel_purchase_28d_view_value",
															ar.getJSONObject(j).getDouble("28d_view"));
												}
												
											}
										}
									}
								}
							}
	
							if (dataset.has("actions")) {
								if (!dataset.isNull("actions")) {
									JSONArray ar = dataset.getJSONArray("actions");
									if (ar.length() != 0) {
										for (int j = 0; j < ar.length(); j++) {
											if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_lead")) {
												if (ar.getJSONObject(j).has("value")) {
													report.put("oc_fb_pixel_lead", ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("oc_fb_pixel_lead_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
	
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("oc_fb_pixel_lead_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
											
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("leadgen.other")) {
												if (ar.getJSONObject(j).has("value")) {
													report.put("leadgenOther", ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("leadgenOther_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("leadgenOther_28d_view",ar.getJSONObject(j).getInt("28d_view"));
												}
												
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_purchase")) {
								
												if (ar.getJSONObject(j).has("value")) {
													report.put("oc_fb_pixel_purchase_value",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("oc_fb_pixel_purchase_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("oc_fb_pixel_purchase_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_view_content")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("fb_pixel_view_content",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("fb_pixel_view_content_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("fb_pixel_view_content_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("landing_page_view")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("landing_page_view",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("landing_page_view_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("landing_page_view_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_add_payment_info")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("fb_pixel_add_payment_info",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("fb_pixel_add_payment_info_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("fb_pixel_add_payment_info_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_add_to_cart")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("fb_pixel_add_to_cart",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("fb_pixel_add_to_cart_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("fb_pixel_add_to_cart_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("offsite_conversion.fb_pixel_initiate_checkout")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("fb_pixel_initiate_checkout",
															ar.getJSONObject(j).getInt("value"));
												}
	
												if (ar.getJSONObject(j).has("28d_click")) {
													report.put("fb_pixel_initiate_checkout_28d_click",
															ar.getJSONObject(j).getInt("28d_click"));
												}
												
												if (ar.getJSONObject(j).has("28d_view")) {
													report.put("fb_pixel_initiate_checkout_28d_view",
															ar.getJSONObject(j).getInt("28d_view"));
												}
	
											} else if (ar.getJSONObject(j).getString("action_type").equals("post_like")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("post_like_value",ar.getJSONObject(j).getInt("value"));
												}
											
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("post_engagement")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("post_engagement",ar.getJSONObject(j).getInt("value"));
												}
											
											} else if (ar.getJSONObject(j).getString("action_type")
													.equals("page_engagement")) {
												
												if (ar.getJSONObject(j).has("value")) {
													report.put("page_engagement", ar.getJSONObject(j).getInt("value"));
												}
												
											}
										}
									}
								}
							}
							
							row.setJson(report);
							App.datachunk.add(row);
							
						}	
							
						return 200;

					} else {
						System.out.println("Response Message : The DATA object is there but is empty.");
						return 200;
					}

				} else {
					System.out.println(
							"Response Message : The DATA object is noty found in the Response. Please check response.");
					return 200;
				}

			} else {
				System.out.println("Response Message : The request failed for fetching AdAccounts.");
				return 200;
			}

		} catch (Exception e) {
			System.out.println("Exception : AdsFetcher - getAdsData()");
			System.out.println(e);
			return 400;
		}

	}
	
}
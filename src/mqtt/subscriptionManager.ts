import mqtt from "mqtt";
import axios from 'axios';
import { config, INDICES, EXPIRY_DATES, STRIKE_RANGE } from "../config";
import * as utils from "../utils";

export const indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"];

// Set of active subscriptions to avoid duplicates
export const activeSubscriptions = new Set<string>();

// Track if we've received the first message for each index
export const isFirstIndexMessage = new Map<string, boolean>();

// Subscribe to all index topics
export function subscribeToAllIndices(client: mqtt.MqttClient) {
  INDICES.forEach((indexName) => {
    const topic = `${config.app.indexPrefix}/${indexName}`;
    console.log(`Subscribing to index: ${topic}`);
    client.subscribe(topic);
    activeSubscriptions.add(topic);
  });
}

// Initialize first message tracking
export function initializeFirstMessageTracking() {
  INDICES.forEach((indexName) => {
    isFirstIndexMessage.set(indexName, true);
  });
}

// Subscribe to options around ATM strike
export async function subscribeToAtmOptions(
  client: mqtt.MqttClient,
  indexName: string,
  atmStrike: number
) {
  // TODO: Implement this function
  // 1. Calculate strike prices around ATM
  // 2. For each strike, get option tokens for CE and PE
  // 3. Subscribe to corresponding topics

  console.log(`Subscribing to ${indexName} options around ATM ${atmStrike}`);

  const strikeDiff = utils.getStrikeDiff(indexName);
  const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
  const strikes = [];

  for (let i = -STRIKE_RANGE; i <= STRIKE_RANGE; i++) {
    strikes.push(atmStrike + i * strikeDiff);
  }

  // TODO: Subscribe to options
  for (const strike of strikes) {
    for (const optionType of ["ce", "pe"] as const) {
      const token = await getOptionToken(indexName, strike, optionType);
      if (token) {
        // const optionTopic = utils.getOptionTopic(indexName, token); // Use utils.getOptionTopic
        const optionTopic = `${config.app.indexPrefix}/NSE_FO|${token}`;
        if (!activeSubscriptions.has(optionTopic)) {
          client.subscribe(optionTopic);
          activeSubscriptions.add(optionTopic);
          console.log(
            `Subscribed to ${optionTopic} (for the option ${indexName}/${expiryDate}/${optionType.toUpperCase()} ${strike})`
          );
        } else {
          console.log(`Already subscribed to ${optionTopic}, skipping`);
        }
      } else {
        console.warn(
          `Failed to fetch token for ${indexName} ${optionType.toUpperCase()} ${strike}`
        );
      }
    }
  }
}

// Fetch option token from API
export async function getOptionToken(
  indexName: string,
  strikePrice: number,
  optionType: "ce" | "pe"
): Promise<string | null> {
  try {
    // // TODO: Implement this function
    // // 1. Make API call to get token
    // // 2. Return the token
    // const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
    // const url = `https://api.trado.trade/token?index=${indexName}&expiryDate=${expiryDate}&optionType=${optionType}&strikePrice=${strikePrice}`;

    // // TODO: Fetch from API and return token

    // return null; // Placeholder
    const expiryDate = EXPIRY_DATES[indexName as keyof typeof EXPIRY_DATES];
    // const url = `https://api.trado.trade/token?index=NIFTY&expiryDate=22-05-2025&optionType=ce&strikePrice=25000`;
    const url = `https://api.trado.trade/token?index=${indexName}&expiryDate=${expiryDate}&optionType=${optionType}&strikePrice=${strikePrice}`;
    
    const response = await axios.get(url);
    // console.log("API response:", response.data);//debugging
    // console.log("API response:", response.data);
    // Assuming the API returns { token: "some-token" }
    // const token = response.data.token;
    const token = response?.data?.data?.token;
    // if (typeof token !== "string") {
    //   throw new Error("Invalid token format from API");
    // }
    // // return token;
    // return token.toString();

    if (token === undefined || token === null) {
      throw new Error("Token is missing in API response");
    }

    return String(token); // works whether it's number or string
  } catch (error) {
    console.error(
      `Error fetching token for ${indexName} ${strikePrice} ${optionType}:`,
      error
    );
    return null;
  }
}

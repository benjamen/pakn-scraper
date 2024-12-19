import * as dotenv from "dotenv";
require('dotenv').config();  // Load the .env file
import axios, { AxiosError } from 'axios';
import { logError, log, colour, validCategories } from "./utilities";
import { Product, UpsertResponse, ProductResponse } from "./typings";

const FRAPPE_URL = process.env.FRAPPE_URL || 'http:/x/api/resource/Product%20Item';
const FRAPPE_API_KEY = process.env.FRAPPE_API_KEY || 'x';
const FRAPPE_API_SECRET = process.env.FRAPPE_API_SECRET || 'x';

const FRAPPE_AUTH = {
  headers: {
    Authorization: `token ${FRAPPE_API_KEY}:${FRAPPE_API_SECRET}`
  }
};
function formatToMySQLDatetime(dateString: string): string {
  // Check if the date is in ISO 8601 format and convert it to 'YYYY-MM-DD HH:MM:SS' format
  const date = new Date(dateString);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

export async function upsertProductToFrappe(scrapedProduct: Product): Promise<UpsertResponse> {
  // Map fields from scraped product to Frappe format
  const category = Array.isArray(scrapedProduct.category) ? scrapedProduct.category.join(', ') : (scrapedProduct.category || '');
  
  const productName = scrapedProduct.name || "Unnamed Product";
  const productId = scrapedProduct.id || "";

  // Format date fields to MySQL-compatible format
  const lastUpdated = formatToMySQLDatetime(scrapedProduct.lastUpdated);
  const lastChecked = formatToMySQLDatetime(scrapedProduct.lastChecked);

  // Transform price_history to a string or another acceptable format (e.g., a JSON string)
  const priceHistory = scrapedProduct.priceHistory && scrapedProduct.priceHistory.length > 0 
    ? JSON.stringify(scrapedProduct.priceHistory)  // Convert array to JSON string
    : '';

  try {
    const currentPrice = parseFloat(scrapedProduct.currentPrice?.toFixed(2)) || 0.00;
    const unitPrice = parseFloat(scrapedProduct.unitPrice?.toFixed(2)) || 0.00;

    // Log the scraped product to check its structure
    console.log("Scraped Product Data:", JSON.stringify(scrapedProduct, null, 2));

    const encodedProductName = encodeURIComponent(productName);

    // Check if the product exists by querying for the productname
    const response = await axios.get(`${FRAPPE_URL}?filters=[["productname", "=", "${encodedProductName}"]]`, FRAPPE_AUTH);

    if (response.data.data && response.data.data.length > 0) {
      // If the product exists, update it
      const dbProduct = response.data.data[0];

      await axios.put(`${FRAPPE_URL}/${dbProduct.name}`, {
        data: {
          productname: productName,
          category: category,
          source_site: scrapedProduct.sourceSite,
          size: scrapedProduct.size,
          unit_price: unitPrice,
          unit_name: scrapedProduct.unitName,
          original_unit_quantity: scrapedProduct.originalUnitQuantity,
          current_price: currentPrice,
          price_history: priceHistory,
          last_updated: lastUpdated,  // Use the formatted datetime
          last_checked: lastChecked,  // Use the formatted datetime
          product_id: productId,
        }
      }, FRAPPE_AUTH);

      return UpsertResponse.Updated;
    } else {
      // If the product does not exist, create a new one
      await axios.post(FRAPPE_URL, {
        data: {
          productname: productName,
          category: category,
          source_site: scrapedProduct.sourceSite,
          size: scrapedProduct.size,
          unit_price: unitPrice,
          unit_name: scrapedProduct.unitName,
          original_unit_quantity: scrapedProduct.originalUnitQuantity,
          current_price: currentPrice,
          price_history: priceHistory,
          last_updated: lastUpdated,  // Use the formatted datetime
          last_checked: lastChecked,  // Use the formatted datetime
          product_id: productId,
        }
      }, FRAPPE_AUTH);

      console.log(`New Product: ${productName.slice(0, 47).padEnd(47)} | $ ${currentPrice}`);
      return UpsertResponse.NewProduct;
    }

  } catch (error) {
    if (error instanceof AxiosError) {
      if (error.response) {
        const statusCode = error.response.status;
        
        if (statusCode === 500) {
          console.log("Server Error Response:", error.response.data); 
          logError(`Server Error: ${error.message}`);
        }

        if (statusCode === 417) {
          console.log("Full Error Response:", error.response);
          logError(`AxiosError: Request failed with status code 417 for product: ${productName}`);
        }
      } else if (error.code === 'ECONNABORTED') {
        logError(`Page Timeout after 15 seconds - Skipping this page: ${productName}`);
      } else {
        logError(`AxiosError: ${error.message}`);
      }
    }

    if (error.response && error.response.status === 404) {
      await axios.post(FRAPPE_URL, {
        data: {
          productname: productName,
          category: category,
          source_site: scrapedProduct.sourceSite,
          size: scrapedProduct.size,
          unit_price: unitPrice,
          unit_name: scrapedProduct.unitName,
          original_unit_quantity: scrapedProduct.originalUnitQuantity,
          current_price: currentPrice,
          price_history: priceHistory,
          last_updated: lastUpdated,  // Use the formatted datetime
          last_checked: lastChecked,  // Use the formatted datetime
          product_id: productId,
        }
      }, FRAPPE_AUTH);

      console.log(`New Product: ${productName.slice(0, 47).padEnd(47)} | $ ${currentPrice}`);
      return UpsertResponse.NewProduct;

    } else {
      logError(error);
      return UpsertResponse.Failed;
    }
  }
}


function buildUpdatedProduct(
  scrapedProduct: Product,
  dbProduct: Product
): ProductResponse {
  let dbDay = dbProduct.lastUpdated.toString();
  dbDay = dbDay.slice(0, 10);
  let scrapedDay = scrapedProduct.lastUpdated.toISOString().slice(0, 10);

  const priceDifference = Math.abs(
    dbProduct.currentPrice - scrapedProduct.currentPrice
  );

  if (priceDifference > 0.05 && dbDay !== scrapedDay) {
    dbProduct.priceHistory.push(scrapedProduct.priceHistory[0]);
    scrapedProduct.priceHistory = dbProduct.priceHistory;
    logPriceChange(dbProduct, scrapedProduct.currentPrice);
    return {
      upsertType: UpsertResponse.PriceChanged,
      product: scrapedProduct,
    };
  } else if (
    !dbProduct.category.every((category) => validCategories.includes(category)) ||
    dbProduct.category === null
  ) {
    console.log(
      `  Categories Changed: ${scrapedProduct.name
        .padEnd(40)
        .substring(0, 40)}` +
        ` - ${dbProduct.category.join(" ")} > ${scrapedProduct.category.join(
          " "
        )}`
    );

    scrapedProduct.priceHistory = dbProduct.priceHistory;
    scrapedProduct.lastUpdated = dbProduct.lastUpdated;

    return {
      upsertType: UpsertResponse.InfoChanged,
      product: scrapedProduct,
    };
  } else {
    dbProduct.lastChecked = scrapedProduct.lastChecked;
    return {
      upsertType: UpsertResponse.AlreadyUpToDate,
      product: dbProduct,
    };
  }
}

export function logPriceChange(product: Product, newPrice: number) {
  const priceIncreased = newPrice > product.currentPrice;
  log(
    priceIncreased ? colour.red : colour.green,
    "  Price " +
      (priceIncreased ? "Up   : " : "Down : ") +
      product.name.slice(0, 47).padEnd(47) +
      " | $" +
      product.currentPrice.toString().padStart(4) +
      " > $" +
      newPrice
  );
}

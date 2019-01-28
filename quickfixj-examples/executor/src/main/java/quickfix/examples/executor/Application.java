/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package quickfix.examples.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ConfigError;
import quickfix.DataDictionaryProvider;
import quickfix.DoNotSend;
import quickfix.FieldConvertError;
import quickfix.FieldNotFound;
import quickfix.FixVersions;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.LogUtil;
import quickfix.Message;
import quickfix.MessageUtils;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.UnsupportedMessageType;
import quickfix.field.Account;
import quickfix.field.ApplVerID;
import quickfix.field.AvgPx;
import quickfix.field.CxlRejResponseTo;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LastShares;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.examples.executor.OrdType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Price;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.MsgType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import java.util.Random;

import quickfix.DefaultMessageFactory;
import quickfix.MessageUtils;
import quickfix.fix43.NewOrderSingle;

public class Application extends quickfix.MessageCracker implements quickfix.Application {
    private static final String DEFAULT_MARKET_PRICE_KEY = "DefaultMarketPrice";
    private static final String ALWAYS_FILL_LIMIT_KEY = "AlwaysFillLimitOrders";
    private static final String VALID_ORDER_TYPES_KEY = "ValidOrderTypes";

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final boolean alwaysFillLimitOrders;
    private final HashSet<String> validOrderTypes = new HashSet<>();
    private MarketDataProvider marketDataProvider;

    public Application(SessionSettings settings) throws ConfigError, FieldConvertError {
        // initializeValidOrderTypes(settings);
        initializeMarketDataProvider(settings);

        alwaysFillLimitOrders = settings.isSetting(ALWAYS_FILL_LIMIT_KEY) && settings.getBool(ALWAYS_FILL_LIMIT_KEY);
    }

    private void initializeMarketDataProvider(SessionSettings settings) throws ConfigError, FieldConvertError {
        if (settings.isSetting(DEFAULT_MARKET_PRICE_KEY)) {
            if (marketDataProvider == null) {
                final double defaultMarketPrice = settings.getDouble(DEFAULT_MARKET_PRICE_KEY);
                marketDataProvider = new MarketDataProvider() {
                    public double getAsk(String symbol) {
                        return defaultMarketPrice;
                    }

                    public double getBid(String symbol) {
                        return defaultMarketPrice;
                    }
                };
            } else {
                log.warn("Ignoring " + DEFAULT_MARKET_PRICE_KEY + " since provider is already defined.");
            }
        }
    }

    // private void initializeValidOrderTypes(SessionSettings settings) throws ConfigError, FieldConvertError {
    //     if (settings.isSetting(VALID_ORDER_TYPES_KEY)) {
    //         List<String> orderTypes = Arrays
    //                 .asList(settings.getString(VALID_ORDER_TYPES_KEY).trim().split("\\s*,\\s*"));
    //         validOrderTypes.addAll(orderTypes);
    //     } else {
    //         validOrderTypes.add(OrdType.LIMIT + "");
    //     }
    // }

    public void onCreate(SessionID sessionID) {
        Session.lookupSession(sessionID).getLog().onEvent("Valid order types: " + validOrderTypes);
    }

    public void onLogon(SessionID sessionID) {
    }

    public void onLogout(SessionID sessionID) {
    }

    public void toAdmin(quickfix.Message message, SessionID sessionID) {
    }

    public void toApp(quickfix.Message message, SessionID sessionID) throws DoNotSend {
    }

    public void fromAdmin(quickfix.Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat,
            IncorrectTagValue, RejectLogon {
    }

    public void fromApp(quickfix.Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat,
            IncorrectTagValue, UnsupportedMessageType {
        // crack(message, sessionID);
        MsgType msgType = new MsgType();
        String type = message.getHeader().getField(msgType).getValue();
        if (type.contains("D")) {
          log.info("custom");
          oncustMessage(((NewOrderSingle)message), sessionID);
        } else {
          crack(message, sessionID);
        }
        // onMessage((NewOrderSingleModified) message, sessionID);
    }

    private boolean isOrderExecutable(Message order, Price price) throws FieldNotFound {
        if (order.getString(40) == "2") {
            BigDecimal limitPrice = new BigDecimal(order.getString(Price.FIELD));
            char side = order.getChar(Side.FIELD);
            BigDecimal thePrice = new BigDecimal("" + price.getValue());

            return (side == Side.BUY && thePrice.compareTo(limitPrice) <= 0)
                    || ((side == Side.SELL || side == Side.SELL_SHORT) && thePrice.compareTo(limitPrice) >= 0);
        }
        return true;
    }

    private Price getPrice(Message message) throws FieldNotFound {
        Price price;
        if (message.getString(40) == "2" && alwaysFillLimitOrders) {
            price = new Price(message.getDouble(Price.FIELD));
        } else {
            if (marketDataProvider == null) {
                throw new RuntimeException("No market data provider specified for market order");
            }
            char side = message.getChar(Side.FIELD);
            if (side == Side.BUY) {
                price = new Price(marketDataProvider.getAsk(message.getString(Symbol.FIELD)));
            } else if (side == Side.SELL || side == Side.SELL_SHORT) {
                price = new Price(marketDataProvider.getBid(message.getString(Symbol.FIELD)));
            } else {
                throw new RuntimeException("Invalid order side: " + side);
            }
        }
        return price;
    }

    private void sendMessage(SessionID sessionID, Message message) {
        try {
            Session session = Session.lookupSession(sessionID);
            if (session == null) {
                throw new SessionNotFound(sessionID.toString());
            }

            DataDictionaryProvider dataDictionaryProvider = session.getDataDictionaryProvider();
            if (dataDictionaryProvider != null) {
                try {
                    dataDictionaryProvider.getApplicationDataDictionary(
                            getApplVerID(session, message)).validate(message, true);
                } catch (Exception e) {
                    LogUtil.logThrowable(sessionID, "Outgoing message failed validation: "
                            + e.getMessage(), e);
                    return;
                }
            }

            session.send(message);
        } catch (SessionNotFound e) {
            log.error(e.getMessage(), e);
        }
    }

    private ApplVerID getApplVerID(Session session, Message message) {
        String beginString = session.getSessionID().getBeginString();
        if (FixVersions.BEGINSTRING_FIXT11.equals(beginString)) {
            return new ApplVerID(ApplVerID.FIX50);
        } else {
            return MessageUtils.toApplVerID(beginString);
        }
    }


    private void validateOrder(Message order) throws IncorrectTagValue, FieldNotFound {
        OrdType ordType = new OrdType(order.getString(40));
        if (!validOrderTypes.contains(ordType.getValue())) {
            log.error("Order type not in ValidOrderTypes setting");
            throw new IncorrectTagValue(ordType.getField());
        }
        if (ordType.getValue() == "1" && marketDataProvider == null) {
            log.error("DefaultMarketPrice setting not specified for market order");
            throw new IncorrectTagValue(ordType.getField());
        }
    }

    public void oncustMessage(NewOrderSingle order, SessionID sessionID) throws FieldNotFound,
            UnsupportedMessageType, IncorrectTagValue {
        try {
        // validateOrder(order);
        log.info("cust message");

        OrderQty orderQty = order.getOrderQty();
        Price price = getPrice(order);

        quickfix.fix43.ExecutionReport accept = new quickfix.fix43.ExecutionReport(
                    genOrderID(), genExecID(), new ExecType(ExecType.FILL), new OrdStatus(
                            OrdStatus.NEW), order.getSide(), new LeavesQty(order.getOrderQty()
                            .getValue()), new CumQty(0), new AvgPx(0));

        accept.set(order.getClOrdID());
        accept.set(order.getSymbol());
        accept.set(order.getAccount());
        try {
          accept.set(order.getPrice());
        } catch (FieldNotFound e) {
          // market price
        }
        accept.set(order.getOrderQty());
        // accept.setField(new OrdType(order.getString(40)));
        // accept.set(order.getOrdType());

        Random rand = new Random();
        int chance = rand.nextInt(200) + 1;

        if (chance == 1) {
          log.info("cust message reject");
          // reject 0.5% of orders
          accept.set(new ExecType(ExecType.REJECTED));
          accept.set(new OrdStatus(OrdStatus.REJECTED));
          sendMessage(sessionID, accept);
        } else if (chance == 2) {
          log.info("cust message miss");
          // intentionally another miss 0.5% of orders
        } else if (chance > 2 && chance < 52 && orderQty.getValue() > 1 && orderQty.getValue() % 2 == 0) {
          log.info("cust message partial fill");
          // only partial fill about 25% of the time, when the order quantity is even
          sendMessage(sessionID, accept);

          if (isOrderExecutable(order, price)) {
              orderQty.setValue( orderQty.getValue() * 0.9 ); // .9 the value
              quickfix.fix43.ExecutionReport executionReport = new quickfix.fix43.ExecutionReport(genOrderID(),
                      genExecID(), new ExecType(ExecType.PARTIAL_FILL), new OrdStatus(OrdStatus.PARTIALLY_FILLED), order.getSide(),
                      new LeavesQty(0), new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

              executionReport.set(order.getClOrdID());
              executionReport.set(order.getSymbol());
              executionReport.set(orderQty);
              executionReport.set(new LastQty(orderQty.getValue()));
              executionReport.set(new LastPx(price.getValue()));
              executionReport.set(order.getAccount());

              sendMessage(sessionID, executionReport);
          }

        } else {
          log.info("cust message fullfil");
          // fully fill 73% of the time
          sendMessage(sessionID, accept);

          if (isOrderExecutable(order, price)) {
              quickfix.fix43.ExecutionReport executionReport = new quickfix.fix43.ExecutionReport(genOrderID(),
                      genExecID(), new ExecType(ExecType.FILL), new OrdStatus(OrdStatus.FILLED), order.getSide(),
                      new LeavesQty(0), new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

              executionReport.set(order.getClOrdID());
              executionReport.set(order.getSymbol());
              executionReport.set(orderQty);
              executionReport.set(new LastQty(orderQty.getValue()));
              executionReport.set(new LastPx(price.getValue()));
              executionReport.set(order.getAccount());

              sendMessage(sessionID, executionReport);

              if (chance > 100) {
                log.info("cust message done for day");
                // randomly done for day
                quickfix.fix43.ExecutionReport done = new quickfix.fix43.ExecutionReport(genOrderID(),
                        genExecID(), new ExecType(ExecType.DONE_FOR_DAY), new OrdStatus(OrdStatus.DONE_FOR_DAY), order.getSide(),
                        new LeavesQty(0), new CumQty(orderQty.getValue()), new AvgPx(price.getValue()));

                done.set(order.getClOrdID());
                done.set(order.getSymbol());
                done.set(orderQty);
                done.set(new LastQty(orderQty.getValue()));
                done.set(new LastPx(price.getValue()));
                done.set(order.getAccount());

                sendMessage(sessionID, executionReport);
              }
          }
        }
        } catch (RuntimeException e) {
            LogUtil.logThrowable(sessionID, e.getMessage(), e);
        }
    }

    public void onMessage(quickfix.fix43.OrderCancelRequest message, SessionID sessionID)
        throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {

        log.info("cancelling");

        quickfix.fix43.ExecutionReport pending = new quickfix.fix43.ExecutionReport(
                    genOrderID(), genExecID(), new ExecType(ExecType.PENDING_CANCEL), new OrdStatus(
                            OrdStatus.PENDING_CANCEL), message.getSide(), new LeavesQty(message.getOrderQty()
                            .getValue()), new CumQty(0), new AvgPx(0));

        pending.set(message.getAccount());
        pending.set(message.getClOrdID());
        pending.set(message.getSymbol());
        pending.set(message.getOrigClOrdID());

        sendMessage(sessionID, pending);

        Random rand = new Random();
        int chance = rand.nextInt(100) + 1;

        if (chance < 50) {
          log.info("cancelling now");
          // cancel half the time
          quickfix.fix43.ExecutionReport cancelled = new quickfix.fix43.ExecutionReport(
                      genOrderID(), genExecID(), new ExecType(ExecType.CANCELED), new OrdStatus(
                              OrdStatus.CANCELED), message.getSide(), new LeavesQty(0), new CumQty(0), new AvgPx(0));

          cancelled.set(message.getAccount());
          cancelled.set(message.getClOrdID());
          cancelled.set(message.getSymbol());
          cancelled.set(message.getOrigClOrdID());

          sendMessage(sessionID, cancelled);
        } else {
          log.info("cancelling reject");
          // reject half the time
          quickfix.fix43.OrderCancelReject rejected = new quickfix.fix43.OrderCancelReject(
                      genOrderID(), message.getClOrdID(), message.getOrigClOrdID(),
                      new OrdStatus(OrdStatus.CANCELED), new CxlRejResponseTo(CxlRejResponseTo.ORDER_CANCEL_REQUEST));

          sendMessage(sessionID, rejected);
        }

    }

    public OrderID genOrderID() {
        return new OrderID(Integer.toString(++m_orderID));
    }

    public ExecID genExecID() {
        return new ExecID(Integer.toString(++m_execID));
    }

    /**
     * Allows a custom market data provider to be specified.
     *
     * @param marketDataProvider
     */
    public void setMarketDataProvider(MarketDataProvider marketDataProvider) {
        this.marketDataProvider = marketDataProvider;
    }

    private int m_orderID = 0;
    private int m_execID = 0;
}

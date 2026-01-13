


TRADE_PRICE_RATE = 0.0002

class Hold:

    def __init__(self, amount: float):
        self.amount = amount
        self.available_amount = float(amount)
        self.holds = dict()  # code -> shares held
        self.trade_history = []  # list of trade dicts
        self.daily_buy_settlement = defaultdict(lambda: defaultdict(int)) # date_key -> code -> quantity. Shares bought on 'date_key' become available on the next trading day.

    def _get_date_key(self, date_time_str):
        # Assuming date_time_str is in format YYYYMMDDHHMMSSmmm
        if not isinstance(date_time_str, str) or len(date_time_str) < 8:
            raise ValueError(f"Invalid date_time_str format: {date_time_str}. Expected YYYYMMDD...")
        return date_time_str[:8]

    def buy(self, code, volumn, price, date_time_str): # Add date_time_str
        """Attempt to buy `volumn` shares of `code` at `price`.

        Adapts volume downward if not enough cash. Returns the executed volume (int).
        """
        vol = int(volumn)
        if vol <= 0 or price <= 0:
            return 0

        cost = float(price) * vol
        commission = abs(cost) * TRADE_PRICE_RATE
        total_needed = cost + commission

        if total_needed > self.available_amount:
            # compute legal maximal volume
            legal_vol = self._get_legal_volumn(code, price)
            vol = int(legal_vol)
            cost = float(price) * vol
            commission = abs(cost) * TRADE_PRICE_RATE
            total_needed = cost + commission
        
        if vol == 0: # If after calculating legal volume, it's 0, then no trade.
            return 0

        # execute
        self.available_amount -= total_needed
        self.holds[code] = self.holds.get(code, 0) + vol
        
        # Record for T+1 settlement
        date_key = self._get_date_key(date_time_str)
        self.daily_buy_settlement[date_key][code] += vol

        self.trade_history.append({
            'action': 'BUY',
            'code': code,
            'volume': vol,
            'price': float(price),
            'cost': total_needed,
            'commission': commission,
            'date': date_time_str # Store full datetime for history
        })
        return vol
    
    def sell(self, code, volumn, price, date_time_str): # Add date_time_str
        """Sell up to `volumn` shares of `code` at `price`. Returns executed volume (int)."""
        vol = int(volumn)
        held_total = int(self.holds.get(code, 0))
        available_to_sell = self.get_available_shares(code, date_time_str) # Use the new method

        if vol <= 0 or held_total <= 0 or price <= 0:
            return 0

        # Only sell what is available for T+1
        if vol > available_to_sell:
            vol = available_to_sell
        
        if vol == 0: # If no shares available to sell, then no trade.
            return 0

        revenue = float(price) * vol
        commission = abs(revenue) * TRADE_PRICE_RATE
        net = revenue - commission

        # execute
        self.available_amount += net
        self.holds[code] = held_total - vol

        self.trade_history.append({
            'action': 'SELL',
            'code': code,
            'volume': vol,
            'price': float(price),
            'revenue': net,
            'commission': commission,
            'date': date_time_str # Store full datetime for history
        })
        return vol

    def _get_legal_volumn(self, code, price) -> float:
        """Return the maximum whole-share volume we can buy for `code` at `price` given available cash."""
        if price <= 0:
            return 0
        # include commission in estimate
        per_share_cost = price * (1.0 + TRADE_PRICE_RATE)
        return int(self.available_amount // per_share_cost)

    def _get_legal_amount(self, code, volumn) -> float:
        """Estimate cash required for `volumn` shares at unknown price; returns a best-effort estimate (zero if unknown)."""
        # We cannot compute precise cash without a price; return 0.0 as placeholder
        try:
            vol = int(volumn)
            return float(vol)
        except Exception:
            return 0.0

    def _check_available(self, code, volumn, price) -> bool:
        """Quick check whether we have enough cash/holds for the requested trade."""
        vol = int(volumn)
        if price <= 0:
            return False
        if vol > 0:
            # buy
            needed = price * vol * (1.0 + TRADE_PRICE_RATE)
            return self.available_amount >= needed
        else:
            # sell
            held = int(self.holds.get(code, 0))
            return held >= abs(vol)
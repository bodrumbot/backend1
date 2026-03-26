// ==========================================
// BODRUM SERVER - FAQAT POLLING
// Payme callback O'CHIRILDI
// ==========================================

const express = require('express');
const { createServer } = require('http');
const { PrismaClient } = require('@prisma/client');
const cors = require('cors');
const path = require('path');

const prisma = new PrismaClient();
const app = express();
const httpServer = createServer(app);

// CORS
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  credentials: true
}));

app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, '../webapp')));

// ===================== API ROUTES =====================

// Health check
app.get('/health', async (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    mode: 'POLLING_ONLY',
    message: 'Payme callback o\'chirildi, faqat polling'
  });
});

// Barcha buyurtmalarni olish
app.get('/api/orders', async (req, res) => {
  try {
    const orders = await prisma.order.findMany({
      where: {
        OR: [
          { paymentStatus: 'paid' },
          { status: 'accepted' },
          { status: 'rejected' }
        ]
      },
      orderBy: { createdAt: 'desc' }
    });
    res.json(orders);
  } catch (error) {
    console.error('Orders error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Yangi buyurtmalarni olish
app.get('/api/orders/new', async (req, res) => {
  try {
    const orders = await prisma.order.findMany({
      where: {
        status: 'pending_payment'
      },
      orderBy: { createdAt: 'desc' }
    });
    res.json(orders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Bitta buyurtma olish
app.get('/api/orders/:orderId', async (req, res) => {
  try {
    const order = await prisma.order.findUnique({
      where: { orderId: req.params.orderId }
    });
    if (!order) return res.status(404).json({ error: 'Not found' });
    res.json(order);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ⭐ TO'LOV STATUSINI YANGILASH (Frontend dan chaqiriladi)
app.post('/api/orders/:orderId/payment', async (req, res) => {
  try {
    const { status, transactionId } = req.body;
    
    const order = await prisma.order.update({
      where: { orderId: req.params.orderId },
      data: {
        paymentStatus: status,
        transactionId: transactionId,
        status: status === 'paid' ? 'pending_payment' : 'rejected',
        paidAt: status === 'paid' ? new Date() : null
      }
    });
    
    res.json({ success: true, order });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Yangi buyurtma yaratish
app.post('/api/orders', async (req, res) => {
  try {
    const order = await prisma.order.create({
      data: {
        ...req.body,
        status: 'pending_payment',
        paymentStatus: 'pending'
      }
    });
    
    res.json({
      ...order,
      polling_started: true,
      message: "Buyurtma yaratildi. To'lov 15 daqiqa ichida amalga oshirilishi kerak."
    });
  } catch (error) {
    console.error('Create order error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Buyurtma yangilash
app.put('/api/orders/:orderId', async (req, res) => {
  try {
    const order = await prisma.order.update({
      where: { orderId: req.params.orderId },
      data: req.body
    });
    
    res.json(order);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ===================== PAYME CALLBACK - O'CHIRILDI =====================
// ❌ Payme callback endi yo'q! Faqat polling ishlatiladi

// ===================== USER PROFILE =====================

app.post('/api/user/profile', async (req, res) => {
  try {
    const { tgId } = req.body;
    
    const profile = await prisma.user.findUnique({
      where: { tgId: parseInt(tgId) }
    });
    
    const orders = await prisma.order.findMany({
      where: { tgId: parseInt(tgId) },
      orderBy: { createdAt: 'desc' }
    });
    
    res.json({ success: true, profile, orders });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/user/save-profile', async (req, res) => {
  try {
    const { tgId, name, phone, username } = req.body;
    
    const profile = await prisma.user.upsert({
      where: { tgId: parseInt(tgId) },
      update: { name, phone, username },
      create: { tgId: parseInt(tgId), name, phone, username }
    });
    
    res.json({ success: true, profile });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`⏰ POLLING ONLY - Payme callback o'chirildi`);
});
// ═══════════════════════════════════════════════════════════════════
// CREDYNAMITA — BACKEND (Render)
//
// Reemplaza a functions/index.js (Cloud Functions), que dejó de poder
// desplegarse cuando el proyecto de Firebase bajó del plan Blaze al
// plan Spark (Spark no permite Cloud Functions).
//
// La LÓGICA DE NEGOCIO es la misma de siempre — lo único que cambia es
// "el cableado":
//   - Los onRequest/onCall de Cloud Functions -> rutas de Express.
//   - Los onSchedule (tareas programadas) -> node-cron, corriendo
//     dentro de este mismo proceso.
//   - Los onDocumentCreated/onDocumentUpdated (triggers de Firestore)
//     -> listeners en tiempo real (onSnapshot), posibles acá porque
//     Render corre esto como un proceso que NUNCA SE APAGA (a
//     diferencia de una Cloud Function, que se despierta y se
//     duerme en cada llamada).
//
// NOTA: la integración con BNC (P2P/C2P) se dejó fuera de esta
// migración a propósito — todavía no estaba terminada ni en uso real,
// así que no hacía falta migrarla ahora. Solo se migró lo que ya
// estaba funcionando en producción (Banco del Tesoro + correos +
// recordatorios).
//
// Sigue usando la MISMA base de datos de Firestore de siempre — no se
// movió ni se perdió ningún dato, solo cambió dónde corre el código.
// ═══════════════════════════════════════════════════════════════════

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const admin = require('firebase-admin');

// ── Inicializar Firebase Admin ────────────────────────────────────
// En Cloud Functions esto era automático. Acá, como corremos en un
// servidor ajeno a Google, hace falta una "cuenta de servicio" — un
// JSON que generas en Firebase Console -> Configuración del proyecto
// -> Cuentas de servicio -> Generar nueva clave privada. Ese JSON
// completo va en la variable de entorno FIREBASE_SERVICE_ACCOUNT_KEY,
// pero codificado en BASE64 (no el JSON crudo) — así se evita que los
// saltos de línea de "private_key" se corrompan al copiar/pegar en el
// panel de Render. Si por algún motivo alguien pega el JSON tal cual
// (sin codificar), igual funciona: se detecta y se usa directo.
function leerCredencialesServicio() {
  const valor = process.env.FIREBASE_SERVICE_ACCOUNT_KEY || '';
  const valorLimpio = valor.trim();
  if (valorLimpio.startsWith('{')) {
    // Alguien pegó el JSON crudo en vez de la versión en base64 — lo
    // aceptamos igual, por si acaso.
    return JSON.parse(valorLimpio);
  }
  const decodificado = Buffer.from(valorLimpio, 'base64').toString('utf8');
  return JSON.parse(decodificado);
}
const credencialesServicio = leerCredencialesServicio();
admin.initializeApp({
  credential: admin.credential.cert(credencialesServicio),
});
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// Reemplazo de "firebase-functions/logger" — el resto del código
// llama logger.log/warn/error igual que antes, así no hubo que tocar
// la lógica de negocio para nada.
const logger = {
  log: (...args) => console.log(...args),
  warn: (...args) => console.warn(...args),
  error: (...args) => console.error(...args),
};

const app = express();
app.use(express.json());
// Render (como Cloud Functions) pone un proxy delante — esto hace que
// req.ip devuelva la IP real de quien llama, no la del proxy interno.
app.set('trust proxy', true);

// CORS solo aplica a llamadas hechas desde un NAVEGADOR — no afecta la
// app nativa (Android/iOS) ni al banco llamando al webhook (eso es
// servidor a servidor).
const ORIGENES_PERMITIDOS = [
  'https://credynamita.com',
  'https://www.credynamita.com',
  'https://merchants.credynamita.com',
  'https://credynamita.web.app',
  'https://creditodynamita.web.app',
  /^http:\/\/localhost:\d+$/,
];
function origenPermitido(origen, callback) {
  if (!origen) return callback(null, true); // servidor-a-servidor (sin Origin) — el banco, por ejemplo
  const ok = ORIGENES_PERMITIDOS.some((o) => (o instanceof RegExp ? o.test(origen) : o === origen));
  callback(null, ok);
}
const corsRestringido = cors({ origin: origenPermitido });
const corsLibre = cors(); // el webhook del banco no necesita restricción de origen

// Tolerancia para considerar que un pago cubre una cuota COMPLETA
// aunque no sea exacto (absorbe pequeñas variaciones de tasa BCV).
const TOLERANCIA_MONTO = 0.03; // 3%

// ── Tasa BCV (cacheada 15 min en memoria) ──────────────────────────
let tasaBcvCache = { valor: null, obtenidaEn: 0 };
const TASA_BCV_CACHE_MINUTOS = 15;

async function obtenerTasaBcvReferencia() {
  const ahora = Date.now();
  const cacheVigente = tasaBcvCache.valor !== null &&
    (ahora - tasaBcvCache.obtenidaEn) < TASA_BCV_CACHE_MINUTOS * 60 * 1000;
  if (cacheVigente) return tasaBcvCache.valor;

  try {
    const resp = await fetch('https://ve.dolarapi.com/v1/dolares/oficial');
    const data = await resp.json();
    if (data?.promedio) {
      const tasa = parseFloat(data.promedio);
      tasaBcvCache = { valor: tasa, obtenidaEn: ahora };
      return tasa;
    }
  } catch (error) {
    logger.warn('No se pudo obtener tasa BCV de referencia, usando fallback', error);
  }
  if (tasaBcvCache.valor !== null) return tasaBcvCache.valor;
  return 54.60;
}

// ── Clave de coincidencia de referencias ───────────────────────────
const LARGO_CLAVE_COINCIDENCIA = 8;
function claveDeCoincidencia(ref) {
  if (!ref) return ref;
  const soloDigitos = String(ref).trim().replace(/[^0-9]/g, '');
  const sinCerosIniciales = soloDigitos.replace(/^0+/, '') || '0';
  return sinCerosIniciales.slice(-LARGO_CLAVE_COINCIDENCIA);
}

async function procesarNotificacionPago({ reference, amount, datosWebhook, banco = 'Tesoro' }) {
  if (!reference) return { resultado: 'sin_referencia' };
  const claveCoincidencia = claveDeCoincidencia(reference);
  const pagosRef = db.collection('pagos');

  const yaExiste = await pagosRef.where('referenciaBancoMatch', '==', claveCoincidencia).limit(1).get();
  if (!yaExiste.empty) return { resultado: 'duplicado' };

  const pendienteSnap = await pagosRef
    .where('referenciaMatch', '==', claveCoincidencia)
    .where('status', '==', 'pendiente_revision')
    .limit(1)
    .get();

  if (pendienteSnap.empty) {
    await pagosRef.add({
      referenciaBanco: reference,
      referenciaBancoMatch: claveCoincidencia,
      status: 'huerfano',
      montoRecibidoBanco: Number(amount) || 0,
      banco,
      datosWebhook,
      recibidoEn: FieldValue.serverTimestamp(),
    });
    return { resultado: 'huerfano' };
  }

  const pagoDocRef = pendienteSnap.docs[0].ref;
  await aplicarPagoEnTransaccion(pagoDocRef, {
    referenciaBanco: reference,
    referenciaBancoMatch: claveCoincidencia,
    montoRecibidoBanco: Number(amount) || 0,
    banco,
    datosWebhook,
  });
  return { resultado: 'aprobado', pagoId: pagoDocRef.id };
}

function simularCascada(montoUsdDisponible, pendientes) {
  let restanteUsd = montoUsdDisponible;
  const plan = [];
  for (const cuota of pendientes) {
    if (restanteUsd <= 0.01) break;
    const saldoActual = Number(cuota.saldoPendiente ?? cuota.monto) || 0;
    if (saldoActual <= 0) continue;

    if (restanteUsd >= saldoActual * (1 - TOLERANCIA_MONTO)) {
      const usadoAqui = Math.min(restanteUsd, saldoActual);
      plan.push({ cuota, numeroCuota: cuota.numeroCuota, abonado: usadoAqui, completa: true, saldoNuevo: 0 });
      restanteUsd -= saldoActual;
    } else {
      const saldoNuevo = Math.max(saldoActual - restanteUsd, 0);
      plan.push({ cuota, numeroCuota: cuota.numeroCuota, abonado: restanteUsd, completa: false, saldoNuevo });
      restanteUsd = 0;
      break;
    }
  }
  const montoAplicado = montoUsdDisponible - restanteUsd;
  const montoExcedente = restanteUsd > 0.01 ? restanteUsd : 0;
  return { plan, montoAplicado, montoExcedente };
}

async function aplicarPagoEnTransaccion(pagoDocRef, datosExtra, overrides = {}) {
  const tasaBcvReferencia = await obtenerTasaBcvReferencia();
  let usuarioIdParaReactivar = null;

  await db.runTransaction(async (transaction) => {
    const pagoSnap = await transaction.get(pagoDocRef);
    const pago = pagoSnap.exists ? pagoSnap.data() : {};

    if (!pagoSnap.exists) throw new Error('El pago ya no existe.');
    if (pago.status !== 'pendiente_revision' && pago.status !== 'monto_no_coincide' && pago.status !== 'huerfano') {
      throw new Error(`El pago ya está en estado "${pago.status}", no se puede aprobar de nuevo.`);
    }

    const usuarioId = overrides.usuarioId || pago.usuarioId;
    const creditoId = overrides.creditoId || pago.creditoId;
    usuarioIdParaReactivar = usuarioId;

    if (!usuarioId || !creditoId) {
      throw new Error('Falta usuarioId/creditoId — obligatorio para aprobar este pago.');
    }

    const usuarioRef = db.collection('usuarios').doc(usuarioId);
    const creditoRef = usuarioRef.collection('creditos').doc(creditoId);
    const cuotasQuery = usuarioRef.collection('cuotas').where('creditoId', '==', creditoId).where('pagado', '==', false);
    const cuotasSnap = await transaction.get(cuotasQuery);

    const pendientes = cuotasSnap.docs
      .map((d) => ({ id: d.id, ref: d.ref, ...d.data() }))
      .sort((a, b) => Number(a.numeroCuota) - Number(b.numeroCuota));

    if (pendientes.length === 0) {
      transaction.update(pagoDocRef, { status: 'sin_cuotas_pendientes', ...datosExtra, actualizadoEn: FieldValue.serverTimestamp() });
      return;
    }

    const montoRecibidoBanco = Number(datosExtra.montoRecibidoBanco ?? pago.montoRecibidoBanco) || 0;
    if (montoRecibidoBanco <= 0) throw new Error('El monto recibido debe ser mayor a cero.');

    let tasaEfectiva = tasaBcvReferencia;
    if (pago.montoBs && pago.montoUsd) {
      tasaEfectiva = Number(pago.montoBs) / Number(pago.montoUsd);
    }
    const montoRecibidoUsd = tasaEfectiva > 0 ? montoRecibidoBanco / tasaEfectiva : 0;
    const { plan, montoAplicado, montoExcedente } = simularCascada(montoRecibidoUsd, pendientes);

    const cuotasAfectadas = [];
    const numerosDeCuotaTocados = new Set();
    for (const paso of plan) {
      numerosDeCuotaTocados.add(paso.numeroCuota);
      if (paso.completa) {
        transaction.update(paso.cuota.ref, {
          pagado: true, saldoPendiente: 0, revisandoPago: false,
          pagoId: pagoDocRef.id, fechaPagoEfectivo: FieldValue.serverTimestamp(),
        });
        transaction.update(creditoRef, {
          [`cuotas.cuota${paso.numeroCuota}.pagada`]: true,
          [`cuotas.cuota${paso.numeroCuota}.pagoId`]: pagoDocRef.id,
        });
      } else {
        transaction.update(paso.cuota.ref, { saldoPendiente: paso.saldoNuevo, revisandoPago: false });
      }
      cuotasAfectadas.push({ numeroCuota: paso.numeroCuota, abonado: paso.abonado, completa: paso.completa });
    }

    for (const cuotaPendiente of pendientes) {
      const yaTocada = numerosDeCuotaTocados.has(cuotaPendiente.numeroCuota);
      const bloqueadaPorEstePago = cuotaPendiente.revisandoPago && cuotaPendiente.pagoIdEnRevision === pagoDocRef.id;
      if (!yaTocada && bloqueadaPorEstePago) {
        transaction.update(cuotaPendiente.ref, { revisandoPago: false });
      }
    }

    const montoTotalARestaurar = montoAplicado + montoExcedente;
    transaction.update(pagoDocRef, {
      status: 'aprobado', usuarioId, creditoId, cuotasAfectadas,
      montoRestaurado: montoTotalARestaurar, montoExcedente, tasaUsada: tasaEfectiva,
      ...datosExtra, aprobadoEn: FieldValue.serverTimestamp(),
    });
    transaction.update(usuarioRef, { limite_disponible: FieldValue.increment(montoTotalARestaurar) });
  });

  // Reactivación automática por mora.
  if (usuarioIdParaReactivar) {
    try {
      const usuarioRef = db.collection('usuarios').doc(usuarioIdParaReactivar);
      const usuarioSnap = await usuarioRef.get();
      const usuario = usuarioSnap.data();

      if (usuario?.creditoSuspendido === true) {
        const hoy = new Date();
        hoy.setHours(0, 0, 0, 0);
        const cuotasSnap = await usuarioRef.collection('cuotas').where('pagado', '==', false).get();
        const siguenVencidas = cuotasSnap.docs.some((d) => {
          const c = d.data();
          if (c.estado === 'ANULADA' || !c.fechaVencimiento?.toDate) return false;
          const fechaVenc = c.fechaVencimiento.toDate();
          fechaVenc.setHours(0, 0, 0, 0);
          const diasVencida = Math.round((hoy - fechaVenc) / (1000 * 60 * 60 * 24));
          return diasVencida >= 3;
        });

        if (!siguenVencidas) {
          await usuarioRef.update({ creditoSuspendido: false, fechaReactivacion: FieldValue.serverTimestamp() });
          logger.log(`Crédito reactivado automáticamente para usuario ${usuarioIdParaReactivar}.`);
          const correoDestino = obtenerCorreoDe(usuario);
          if (correoDestino) {
            await enviarCorreo({
              to: correoDestino,
              subject: '✅ Tu crédito fue reactivado',
              html: `${ENCABEZADO_CORREO(`¡Ya estás al día, ${usuario.nombre || ''}!`)}<p>Pusiste al día tus cuotas vencidas — tu crédito disponible quedó <strong>reactivado</strong>. Ya puedes volver a comprar con Credynamita.</p>${PIE_CORREO}`,
            });
          }
          if (usuario?.expoPushToken) {
            await enviarPush({ to: usuario.expoPushToken, title: '¡Crédito reactivado!', body: 'Ya estás al día. Tu crédito disponible vuelve a estar activo.' });
          }
        }
      }
    } catch (error) {
      logger.error(`Error revisando reactivación por mora para ${usuarioIdParaReactivar}:`, error);
    }
  }
}

// ── Verificación de staff (reemplaza "exigirStaff" de onCall) ──────
// En Cloud Functions, onCall traía "request.auth" ya verificado solo.
// Acá lo hacemos a mano: el cliente manda el ID Token de Firebase Auth
// en el header "Authorization: Bearer <token>", y lo verificamos con
// el Admin SDK antes de seguir.
async function verificarStaff(req) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
  if (!token) {
    const err = new Error('Debes iniciar sesión.');
    err.status = 401;
    throw err;
  }
  let decoded;
  try {
    decoded = await admin.auth().verifyIdToken(token);
  } catch (error) {
    const err = new Error('Sesión inválida o vencida.');
    err.status = 401;
    throw err;
  }
  const adminSnap = await db.collection('admins').doc(decoded.uid).get();
  if (!adminSnap.exists) {
    const err = new Error('No tienes permisos de staff.');
    err.status = 403;
    throw err;
  }
  return { uid: decoded.uid, datos: adminSnap.data() };
}

// ── Límite de peticiones por IP (rate limiting) ────────────────────
const BLOQUEAR_POR_IP = true;
const IPS_PERMITIDAS_BANCO = ['190.202.9.210'];

function obtenerIpReal(req) {
  if (req.ip) return req.ip.replace('::ffff:', '');
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) return String(forwarded).split(',')[0].trim();
  return null;
}

async function verificarLimiteDePeticiones(nombreFuncion, ip, maxIntentos = 8, ventanaMinutos = 15) {
  const ipUsada = ip || 'desconocida';
  const clave = `${nombreFuncion}_${ipUsada}`.replace(/[^a-zA-Z0-9_.-]/g, '_');
  const ref = db.collection('rate_limits').doc(clave);
  const ahora = Date.now();
  const ventanaMs = ventanaMinutos * 60 * 1000;

  try {
    return await db.runTransaction(async (t) => {
      const snap = await t.get(ref);
      if (!snap.exists) {
        t.set(ref, { count: 1, windowStart: ahora, actualizadoEn: FieldValue.serverTimestamp() });
        return true;
      }
      const data = snap.data();
      if (ahora - (data.windowStart || 0) > ventanaMs) {
        t.set(ref, { count: 1, windowStart: ahora, actualizadoEn: FieldValue.serverTimestamp() });
        return true;
      }
      if ((data.count || 0) >= maxIntentos) return false;
      t.update(ref, { count: (data.count || 0) + 1, actualizadoEn: FieldValue.serverTimestamp() });
      return true;
    });
  } catch (error) {
    logger.error(`verificarLimiteDePeticiones: error verificando límite para ${clave}:`, error);
    return true;
  }
}

// ═══════════════════════════════════════════════════════════════════
// RUTAS HTTP
// ═══════════════════════════════════════════════════════════════════

/**
 * Webhook de notificación de pago móvil en línea del Banco del Tesoro.
 */
app.post('/webhookPagoMovil', corsLibre, async (req, res) => {
  const {
    payment_type, reference, source_bank, amount,
    source_phone, payment_date, document, source_account, description,
  } = req.body || {};

  const ipSolicitante = obtenerIpReal(req);
  const ipAutorizada = ipSolicitante && IPS_PERMITIDAS_BANCO.includes(ipSolicitante);
  if (!ipAutorizada) {
    logger.warn(`Webhook: IP no reconocida (${ipSolicitante || 'desconocida'}) — ${BLOQUEAR_POR_IP ? 'RECHAZADA' : 'permitida igual (modo solo registrar)'}.`);
    if (BLOQUEAR_POR_IP) {
      return res.status(403).json({ status: 403, message: 'IP no autorizada' });
    }
  }

  if (!reference || amount === undefined) {
    return res.status(400).json({ status: 400, message: 'Faltan campos obligatorios.' });
  }

  try {
    const resultado = await procesarNotificacionPago({
      reference,
      amount,
      datosWebhook: { payment_type, source_bank, source_phone, payment_date, document, source_account, description },
      banco: 'Tesoro',
    });

    if (resultado.resultado === 'duplicado') {
      logger.log(`Referencia ${reference} ya fue procesada antes, respondiendo 400 (duplicado)`);
      return res.status(400).json({ status: 400, message: 'Operación duplicada.' });
    }
    if (resultado.resultado === 'huerfano') {
      logger.log(`Webhook huérfano: referencia ${reference} sin match en la app`);
    } else {
      logger.log(`Pago aplicado: referencia ${reference}`);
    }
    return res.status(200).json({ status: 200 });
  } catch (error) {
    logger.error('Error procesando webhook del Tesoro:', error);
    return res.status(500).json({ status: 500, message: 'Error interno.' });
  }
});

/**
 * La app la llama justo después de que el cliente reporta un pago, para
 * confirmar en segundos si ya llegó del banco (sin esperar el próximo
 * ciclo de revisarPagosNoEnviados).
 */
app.post('/verificarPagoInmediato', corsRestringido, async (req, res) => {
  const { pagoId, referencia } = req.body || {};
  if (!pagoId || !referencia) {
    return res.status(400).json({ error: 'Falta pagoId o referencia.' });
  }
  const claveCoincidencia = claveDeCoincidencia(referencia);

  try {
    const pagoRef = db.collection('pagos').doc(pagoId);
    const pagoSnap = await pagoRef.get();
    if (!pagoSnap.exists) return res.status(404).json({ error: 'El pago no existe.' });
    const pago = pagoSnap.data();

    if (pago.status === 'aprobado') return res.status(200).json({ resultado: 'aprobado' });
    if (pago.status === 'monto_no_coincide') return res.status(200).json({ resultado: 'monto_no_coincide' });
    if (pago.status !== 'pendiente_revision') return res.status(200).json({ resultado: 'esperando' });

    const huerfanoSnap = await db.collection('pagos')
      .where('referenciaBancoMatch', '==', claveCoincidencia)
      .where('status', '==', 'huerfano')
      .limit(1)
      .get();

    if (huerfanoSnap.empty) return res.status(200).json({ resultado: 'esperando' });

    const huerfanoDoc = huerfanoSnap.docs[0];
    const huerfano = huerfanoDoc.data();
    const montoRecibido = Number(huerfano.montoRecibidoBanco) || 0;

    await aplicarPagoEnTransaccion(pagoRef, {
      referenciaBanco: huerfano.referenciaBanco || referencia,
      referenciaBancoMatch: claveCoincidencia,
      montoRecibidoBanco: montoRecibido,
      datosWebhook: huerfano.datosWebhook || null,
    });

    await huerfanoDoc.ref.update({ status: 'fusionado', fusionadoConPagoId: pagoId, actualizadoEn: FieldValue.serverTimestamp() });
    return res.status(200).json({ resultado: 'aprobado' });
  } catch (error) {
    logger.error('Error en verificarPagoInmediato:', error);
    return res.status(500).json({ error: 'Error interno.' });
  }
});

app.post('/obtenerPreRegistroPorCodigo', corsRestringido, async (req, res) => {
  const { codigo } = req.body || {};
  if (!codigo) return res.status(400).json({ error: 'Falta el campo codigo.' });

  const permitido = await verificarLimiteDePeticiones('obtenerPreRegistroPorCodigo', obtenerIpReal(req));
  if (!permitido) return res.status(429).json({ error: 'Demasiados intentos. Intenta de nuevo en unos minutos.' });

  try {
    const snap = await db.collection('pre_registros').where('codigoCorto', '==', String(codigo).trim()).limit(1).get();
    if (snap.empty) return res.status(200).json({ encontrado: false });
    const doc = snap.docs[0];
    return res.status(200).json({ encontrado: true, preRegistro: { ...doc.data(), id_seguro: doc.id } });
  } catch (error) {
    logger.error('Error buscando pre-registro por código:', error);
    return res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/obtenerOrdenPorCodigo', corsRestringido, async (req, res) => {
  const { codigo } = req.body || {};
  if (!codigo) return res.status(400).json({ error: 'Falta el campo codigo.' });

  const permitido = await verificarLimiteDePeticiones('obtenerOrdenPorCodigo', obtenerIpReal(req));
  if (!permitido) return res.status(429).json({ error: 'Demasiados intentos. Intenta de nuevo en unos minutos.' });

  try {
    const codigoLimpio = String(codigo).replace(/[^0-9]/g, '');
    const snap = await db.collection('ordenes').where('codigoOrden', '==', codigoLimpio).limit(1).get();
    if (snap.empty) return res.status(200).json({ encontrado: false });
    const doc = snap.docs[0];
    return res.status(200).json({ encontrado: true, orden: { ...doc.data(), id: doc.id } });
  } catch (error) {
    logger.error('Error buscando orden por código:', error);
    return res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/verificarCedulaDuplicada', corsRestringido, async (req, res) => {
  const { cedula } = req.body || {};
  if (!cedula) return res.status(400).json({ error: 'Falta el campo cedula.' });

  const permitido = await verificarLimiteDePeticiones('verificarCedulaDuplicada', obtenerIpReal(req));
  if (!permitido) return res.status(429).json({ error: 'Demasiados intentos. Intenta de nuevo en unos minutos.' });

  try {
    const snap = await db.collection('usuarios').where('cedula', '==', cedula).limit(1).get();
    return res.status(200).json({ existe: !snap.empty });
  } catch (error) {
    logger.error('Error verificando cédula duplicada:', error);
    return res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/obtenerCorreoPorCedula', corsRestringido, async (req, res) => {
  const { cedula } = req.body || {};
  if (!cedula) return res.status(400).json({ error: 'Falta el campo cedula.' });

  const permitido = await verificarLimiteDePeticiones('obtenerCorreoPorCedula', obtenerIpReal(req), 5, 15);
  if (!permitido) return res.status(429).json({ error: 'Demasiados intentos. Intenta de nuevo en unos minutos.' });

  try {
    const snap = await db.collection('usuarios').where('cedula', '==', cedula).limit(1).get();
    if (snap.empty) return res.status(200).json({ encontrado: false });
    const datos = snap.docs[0].data();
    return res.status(200).json({ encontrado: true, correo: datos.correo || null, uid: datos.uid || snap.docs[0].id });
  } catch (error) {
    logger.error('Error obteniendo correo por cédula:', error);
    return res.status(500).json({ error: 'Error interno' });
  }
});

app.post('/verificarUsuarioExiste', corsRestringido, async (req, res) => {
  const { uid } = req.body || {};
  if (!uid) return res.status(400).json({ error: 'Falta el campo uid.' });

  const permitido = await verificarLimiteDePeticiones('verificarUsuarioExiste', obtenerIpReal(req));
  if (!permitido) return res.status(429).json({ error: 'Demasiados intentos. Intenta de nuevo en unos minutos.' });

  try {
    const snap = await db.collection('usuarios').doc(uid).get();
    return res.status(200).json({ existe: snap.exists });
  } catch (error) {
    logger.error('Error verificando existencia de usuario:', error);
    return res.status(500).json({ error: 'Error interno' });
  }
});

// ── Rutas de staff (reemplazan los onCall — el merchant-web ahora les
// pega directo con fetch(), mandando el ID Token en el header
// Authorization) ────────────────────────────────────────────────────

app.post('/prepararAprobacionHuerfano', corsRestringido, async (req, res) => {
  try {
    await verificarStaff(req);
    const { pagoId } = req.body || {};
    if (!pagoId) return res.status(400).json({ error: 'Falta pagoId.' });

    const pagoSnap = await db.collection('pagos').doc(pagoId).get();
    if (!pagoSnap.exists) return res.status(404).json({ error: 'Este pago ya no existe.' });
    const pago = pagoSnap.data();
    if (pago.status !== 'huerfano') return res.status(409).json({ error: 'Este pago ya no está huérfano.' });

    const documentoBanco = pago.datosWebhook?.document || null;
    if (!documentoBanco) return res.status(200).json({ cliente: null, motivo: 'sin_documento' });

    const letra = documentoBanco.match(/^[A-Za-z]/)?.[0] || '';
    const soloDigitos = documentoBanco.replace(/^[A-Za-z]/, '');
    const digitosSinCeros = soloDigitos.replace(/^0+/, '');
    const variantes = Array.from(new Set([documentoBanco, `${letra}${digitosSinCeros}`, soloDigitos, digitosSinCeros]));

    let usuarioSnap = null;
    for (const variante of variantes) {
      if (!variante) continue;
      const snap = await db.collection('usuarios').where('cedula', '==', variante).limit(1).get();
      if (!snap.empty) { usuarioSnap = snap; break; }
    }
    if (!usuarioSnap || usuarioSnap.empty) return res.status(200).json({ cliente: null, motivo: 'no_encontrado', documentoBanco });

    const usuarioDoc = usuarioSnap.docs[0];
    const usuario = usuarioDoc.data();
    const uid = usuarioDoc.id;

    const creditosSnap = await db.collection('usuarios').doc(uid).collection('creditos').get();
    const tasaBcv = await obtenerTasaBcvReferencia();
    const montoRecibidoBanco = Number(pago.montoRecibidoBanco) || 0;
    const montoRecibidoUsdEstimado = tasaBcv > 0 ? montoRecibidoBanco / tasaBcv : 0;

    const creditos = [];
    for (const creditoDoc of creditosSnap.docs) {
      const credito = creditoDoc.data();
      const estado = String(credito.estado || '').toUpperCase().trim();
      if (estado === 'ANULADA' || estado === 'CANCELADA') continue;

      const cuotasSnap = await db.collection('usuarios').doc(uid).collection('cuotas')
        .where('creditoId', '==', creditoDoc.id).where('pagado', '==', false).get();
      const pendientes = cuotasSnap.docs.map((d) => ({ id: d.id, ...d.data() })).sort((a, b) => Number(a.numeroCuota) - Number(b.numeroCuota));
      if (pendientes.length === 0) continue;

      const { plan, montoAplicado, montoExcedente } = simularCascada(montoRecibidoUsdEstimado, pendientes);
      creditos.push({
        creditoId: creditoDoc.id,
        comercio: credito.comercio || null,
        montoTotal: credito.montoTotal || null,
        fecha: credito.fecha?.toDate ? credito.fecha.toDate().toISOString() : null,
        cuotasPendientes: pendientes.map((c) => ({ id: c.id, numeroCuota: c.numeroCuota, monto: c.monto, saldoPendiente: c.saldoPendiente ?? c.monto })),
        vistaPrevia: plan.map((p) => ({ numeroCuota: p.numeroCuota, abonado: p.abonado, completa: p.completa, saldoNuevo: p.saldoNuevo })),
        montoAplicado, montoExcedente,
      });
    }

    return res.status(200).json({
      cliente: { uid, nombre: usuario.nombre || null, cedula: usuario.cedula || null },
      montoRecibidoBanco, tasaBcvUsada: tasaBcv, creditos,
    });
  } catch (error) {
    return res.status(error.status || 500).json({ error: error.message || 'Error interno.' });
  }
});

app.post('/aprobarPagoManual', corsRestringido, async (req, res) => {
  try {
    const staff = await verificarStaff(req);
    const { pagoId, usuarioId, creditoId } = req.body || {};
    if (!pagoId) return res.status(400).json({ error: 'Falta pagoId.' });

    const pagoRef = db.collection('pagos').doc(pagoId);
    const pagoSnap = await pagoRef.get();
    if (!pagoSnap.exists) return res.status(404).json({ error: 'Este pago ya no existe.' });
    const pago = pagoSnap.data();

    if (pago.status === 'huerfano' && (!usuarioId || !creditoId)) {
      return res.status(400).json({ error: 'Este pago es huérfano: debes indicar a qué usuarioId y creditoId corresponde.' });
    }

    await aplicarPagoEnTransaccion(
      pagoRef,
      { aprobadoManualPor: staff.uid, aprobadoManualNombre: staff.datos.nombre || null },
      { usuarioId, creditoId }
    );
    logger.log(`Pago ${pagoId} aprobado manualmente por ${staff.uid}`);
    return res.status(200).json({ ok: true });
  } catch (error) {
    logger.error('Error aprobando pago manual:', error);
    return res.status(error.status || 500).json({ error: error.message || 'No se pudo aprobar el pago.' });
  }
});

app.post('/rechazarPagoManual', corsRestringido, async (req, res) => {
  try {
    const staff = await verificarStaff(req);
    const { pagoId, motivo } = req.body || {};
    if (!pagoId) return res.status(400).json({ error: 'Falta pagoId.' });

    const pagoRef = db.collection('pagos').doc(pagoId);
    const pagoSnap = await pagoRef.get();
    if (!pagoSnap.exists) return res.status(404).json({ error: 'Este pago ya no existe.' });
    const pago = pagoSnap.data();

    const batch = db.batch();
    batch.update(pagoRef, {
      status: 'rechazado', motivoRechazo: motivo || null,
      rechazadoPor: staff.uid, rechazadoEn: FieldValue.serverTimestamp(),
    });

    if (pago.usuarioId && pago.creditoId) {
      const cuotasSnap = await db.collection('usuarios').doc(pago.usuarioId).collection('cuotas')
        .where('creditoId', '==', pago.creditoId).where('pagoIdEnRevision', '==', pagoId).get();
      cuotasSnap.docs.forEach((d) => batch.update(d.ref, { revisandoPago: false, pagoIdEnRevision: FieldValue.delete() }));
    }

    await batch.commit();
    logger.log(`Pago ${pagoId} rechazado por ${staff.uid}`);
    return res.status(200).json({ ok: true });
  } catch (error) {
    logger.error('Error rechazando pago manual:', error);
    return res.status(error.status || 500).json({ error: error.message || 'No se pudo rechazar el pago.' });
  }
});

// Servir archivos estáticos (como el APK) — Firebase Hosting en el
// plan Spark bloquea archivos ejecutables (.apk incluido), así que el
// APK se sirve desde acá en vez de desde el merchant-web. Cualquier
// archivo que pongas en la carpeta "public" de este proyecto queda
// disponible en https://credynamita-server.onrender.com/nombre-archivo
app.use(express.static('public'));

// Verificación rápida de que el servidor está vivo (útil para que
// Render confirme que el servicio arrancó bien).
app.get('/', (req, res) => res.status(200).send('Credynamita backend OK'));

// ═══════════════════════════════════════════════════════════════════
// TAREA PROGRAMADA: revisar pagos no enviados por el banco (respaldo)
// ═══════════════════════════════════════════════════════════════════
async function revisarPagosNoEnviados() {
  const appUser = process.env.BT_APP_USER;
  const appKey = process.env.BT_APP_KEY;
  const codSocio = process.env.BT_COD_SOCIO;
  const baseUrl = process.env.BT_BASE_URL || 'https://idpagosdes.bt.com.ve:8084/idpagos/com/services';

  if (!appUser || !appKey || !codSocio) {
    logger.warn('revisarPagosNoEnviados: faltan credenciales del banco (BT_APP_USER/BT_APP_KEY/BT_COD_SOCIO) en las variables de entorno.');
    return;
  }

  try {
    const tokenResp = await fetch(`${baseUrl}/consulta/credenciales`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ appUser, appKey, codSocio }),
    });
    const tokenData = await tokenResp.json();
    if (tokenData.status !== 'OK' || !tokenData.token) {
      logger.error('revisarPagosNoEnviados: no se pudo obtener token del banco', tokenData);
      return;
    }

    const noEnviadasResp = await fetch(`${baseUrl}/consulta/noEnviadas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${tokenData.token}` },
      body: JSON.stringify({ canal: '1', codSocio }),
    });
    const noEnviadasData = await noEnviadasResp.json();
    const movimientos = noEnviadasData.movimientos || [];

    if (movimientos.length === 0) {
      logger.log('revisarPagosNoEnviados: sin operaciones pendientes por revisar.');
      return;
    }
    logger.log(`revisarPagosNoEnviados: procesando ${movimientos.length} operación(es).`);

    for (const mov of movimientos) {
      try {
        const resultado = await procesarNotificacionPago({
          reference: mov.referencia,
          amount: Number(mov.monto),
          datosWebhook: {
            payment_type: 'P2C', source_bank: mov.banco || null, source_phone: mov.telefono || null,
            payment_date: mov.fecha && mov.hora ? `${mov.fecha} ${mov.hora}` : null,
            document: mov.nacionalidad && mov.cedula ? `${mov.nacionalidad}${mov.cedula}` : null,
            source_account: mov.nroCuenta || null, description: mov.motivo || null,
          },
        });
        logger.log(`revisarPagosNoEnviados: referencia ${mov.referencia} -> ${resultado.resultado}`);
      } catch (errorMov) {
        logger.error(`revisarPagosNoEnviados: error procesando referencia ${mov.referencia}:`, errorMov);
      }
    }
  } catch (error) {
    logger.error('revisarPagosNoEnviados: error general', error);
  }
}

// ═══════════════════════════════════════════════════════════════════
// NOTIFICACIONES POR CORREO — llamamos directo a la API REST de
// SendGrid (SENDGRID_API_KEY en el .env).
// ═══════════════════════════════════════════════════════════════════
function obtenerCorreoDe(objeto) {
  return objeto?.correo || objeto?.email || null;
}

const SENDGRID_API_KEY = process.env.SENDGRID_API_KEY;
const SENDGRID_FROM = process.env.SENDGRID_FROM || 'dynamitacredit@gmail.com';

async function enviarCorreo({ to, subject, html }) {
  if (!to) { logger.warn('enviarCorreo: no se pudo enviar, falta el destinatario.'); return; }
  if (!SENDGRID_API_KEY) { logger.error('enviarCorreo: falta SENDGRID_API_KEY — no se puede enviar nada.'); return; }

  try {
    const respuesta = await fetch('https://api.sendgrid.com/v3/mail/send', {
      method: 'POST',
      headers: { Authorization: `Bearer ${SENDGRID_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: to }] }],
        from: { email: SENDGRID_FROM, name: 'Credynamita' },
        subject,
        content: [{ type: 'text/html', value: html }],
      }),
    });
    if (respuesta.status >= 200 && respuesta.status < 300) {
      logger.log(`enviarCorreo: enviado correctamente a ${to} (status ${respuesta.status}).`);
    } else {
      const cuerpoError = await respuesta.text();
      logger.error(`enviarCorreo: SendGrid respondió ${respuesta.status} para ${to}: ${cuerpoError}`);
    }
  } catch (error) {
    logger.error(`enviarCorreo: error de red llamando a SendGrid (para ${to}):`, error);
  }
}

async function enviarPush({ to, title, body }) {
  if (!to) return;
  try {
    const respuesta = await fetch('https://exp.host/--/api/v2/push/send', {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify({ to, sound: 'default', title, body }),
    });
    const resultado = await respuesta.json();
    if (resultado?.data?.status === 'error') {
      logger.error(`enviarPush: Expo reportó error para ${to}:`, resultado.data);
    } else {
      logger.log(`enviarPush: notificación enviada a ${to}.`);
    }
  } catch (error) {
    logger.error(`enviarPush: error de red enviando a ${to}:`, error);
  }
}

const LOGO_URL = 'https://credynamita.com/logo-email.png';
const URL_APP = 'https://credynamita.com';

const ENCABEZADO_CORREO = (titulo) => `
  <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto; font-size: 15px;">
    <div style="background-color:#DC2626; padding:24px; border-radius:16px 16px 0 0; text-align:center;">
      <img src="${LOGO_URL}" alt="Credynamita" style="max-width:180px; height:auto;" />
    </div>
    <div style="background-color:#F4F7FE; padding:28px; border-radius:0 0 16px 16px;">
      <h2 style="color:#1B2559; margin-top:0; font-size:22px;">${titulo}</h2>
`;
const PIE_CORREO = `
    </div>
    <div style="text-align:center; padding:22px 10px 10px 10px;">
      <p style="margin:4px 0;">
        <a href="${URL_APP}" style="color:#DC2626; text-decoration:none; font-weight:bold; font-size:15px;">credynamita.com</a>
      </p>
      <p style="margin:4px 0; font-size:14px; color:#475569;">Descarga nuestra app</p>
      <p style="margin:14px 0 0 0; font-size:11px; color:#94a3b8;">
        Copyright © 2026 CREDYNAMITA RIF J-508266587 DYNAMITA CREDIT GROUP, C.A.
      </p>
    </div>
  </div>
`;

// ── Cada una de estas funciones es la MISMA lógica que antes vivía en
// un trigger de Firestore — ahora las llaman los listeners de más
// abajo, en vez de que Firebase las dispare solo. ────────────────────

async function correoBienvenida(uid, usuario) {
  if (!obtenerCorreoDe(usuario)) {
    logger.warn(`correoBienvenida: el usuario ${uid} no tiene campo "correo"/"email", no se envía nada.`);
    return;
  }
  await enviarCorreo({
    to: obtenerCorreoDe(usuario),
    subject: '¡Bienvenido a Credynamita! Tu crédito ya está activo',
    html: `${ENCABEZADO_CORREO(`¡Hola ${usuario.nombre || ''}!`)}<p>Tu cuenta en Credynamita ya está activa y lista para usar.</p><p style="font-size:18px;"><strong>Límite de crédito aprobado:</strong><br/><span style="font-size:28px; color:#10B981; font-weight:800;">$${Number(usuario.limite_total || 0).toFixed(2)}</span></p><p>Ya puedes usarlo en cualquiera de nuestras tiendas afiliadas.</p>${PIE_CORREO}`,
  });
}

async function correoCambioLimite(usuario, limiteAnterior) {
  if (!obtenerCorreoDe(usuario)) return;
  if (Number(limiteAnterior) === Number(usuario.limite_total)) return;
  await enviarCorreo({
    to: obtenerCorreoDe(usuario),
    subject: 'Tu límite de crédito fue actualizado',
    html: `${ENCABEZADO_CORREO(`Hola ${usuario.nombre || ''}`)}<p>Tu nuevo límite de crédito aprobado es:</p><p style="font-size:28px; color:#10B981; font-weight:800;">$${Number(usuario.limite_total || 0).toFixed(2)}</p>${PIE_CORREO}`,
  });
}

async function correoNuevaCompra(uid, creditoId, credito) {
  const usuarioSnap = await db.collection('usuarios').doc(uid).get();
  const usuario = usuarioSnap.data();
  if (!obtenerCorreoDe(usuario)) return;

  const cuotasSnap = await db.collection('usuarios').doc(uid).collection('cuotas').where('creditoId', '==', creditoId).get();
  const cuotas = cuotasSnap.docs.map((d) => d.data()).sort((a, b) => Number(a.numeroCuota) - Number(b.numeroCuota));
  const filasCuotas = cuotas.map((c) => `<tr><td style="padding:8px 10px;border-bottom:1px solid #E0E5F2;">Cuota #${c.numeroCuota}</td><td style="padding:8px 10px;border-bottom:1px solid #E0E5F2;">$${Number(c.monto).toFixed(2)}</td><td style="padding:8px 10px;border-bottom:1px solid #E0E5F2;">${c.fechaVencimiento?.toDate ? c.fechaVencimiento.toDate().toLocaleDateString('es-ES') : '—'}</td></tr>`).join('');

  await enviarCorreo({
    to: obtenerCorreoDe(usuario),
    subject: `Compra registrada en ${credito.comercio || 'Credynamita'}`,
    html: `${ENCABEZADO_CORREO(`¡Gracias por tu compra, ${usuario.nombre || ''}!`)}<p><strong>Comercio:</strong> ${credito.comercio || '—'}</p><p><strong>Monto total:</strong> $${Number(credito.montoTotal || 0).toFixed(2)}</p><p><strong>Inicial pagado:</strong> $${Number(credito.pagoInicial || 0).toFixed(2)}</p><p style="margin-top:20px;"><strong>Tu plan de pago:</strong></p><table style="border-collapse:collapse; width:100%; background:white; border-radius:8px; overflow:hidden;"><thead><tr style="background:#E0E5F2;"><th style="text-align:left; padding:8px 10px;">Cuota</th><th style="text-align:left; padding:8px 10px;">Monto</th><th style="text-align:left; padding:8px 10px;">Vence</th></tr></thead><tbody>${filasCuotas}</tbody></table>${PIE_CORREO}`,
  });
}

async function correoEstadoPago(despues) {
  if (!despues.usuarioId) return;
  const usuarioSnap = await db.collection('usuarios').doc(despues.usuarioId).get();
  const usuario = usuarioSnap.data();
  if (!obtenerCorreoDe(usuario)) return;

  if (despues.status === 'aprobado') {
    const cuotasTexto = Array.isArray(despues.cuotasAfectadas) && despues.cuotasAfectadas.length > 0
      ? despues.cuotasAfectadas.map((c) => `Cuota #${c.numeroCuota}${c.completa ? ' — pagada completa' : ' — abono parcial'}`).join('<br/>')
      : 'Tu crédito fue actualizado.';
    await enviarCorreo({
      to: obtenerCorreoDe(usuario),
      subject: '✅ Tu pago fue aprobado',
      html: `${ENCABEZADO_CORREO(`¡Pago confirmado, ${usuario.nombre || ''}!`)}<p style="font-size:24px; color:#10B981; font-weight:800;">$${Number(despues.montoRestaurado || 0).toFixed(2)}</p><p>${cuotasTexto}</p><p style="color:#64748b; font-size:12px;">Referencia: ${despues.referencia || despues.referenciaBanco || '—'}</p>${PIE_CORREO}`,
    });
  } else {
    await enviarCorreo({
      to: obtenerCorreoDe(usuario),
      subject: '⚠️ Tu pago no pudo ser confirmado',
      html: `${ENCABEZADO_CORREO(`Hola ${usuario.nombre || ''}`)}<p>El pago con referencia <strong>${despues.referencia || despues.referenciaBanco || '—'}</strong> no pudo ser confirmado.</p>${despues.motivoRechazo ? `<p><strong>Motivo:</strong> ${despues.motivoRechazo}</p>` : ''}<p>Si crees que esto es un error, contacta a soporte.</p>${PIE_CORREO}`,
    });
  }
}

async function correoCompraSaldada(uid, creditoId) {
  const cuotasSnap = await db.collection('usuarios').doc(uid).collection('cuotas').where('creditoId', '==', creditoId).get();
  const todasResueltas = cuotasSnap.docs.every((d) => {
    const c = d.data();
    return c.pagado || c.estado === 'ANULADA';
  });
  if (!todasResueltas) return;

  const creditoRef = db.collection('usuarios').doc(uid).collection('creditos').doc(creditoId);
  let debeEnviar = false;
  await db.runTransaction(async (t) => {
    const creditoSnap = await t.get(creditoRef);
    if (!creditoSnap.exists || creditoSnap.data().correoSaldadoEnviado) return;
    t.update(creditoRef, { correoSaldadoEnviado: true });
    debeEnviar = true;
  });
  if (!debeEnviar) return;

  const usuarioSnap = await db.collection('usuarios').doc(uid).get();
  const usuario = usuarioSnap.data();
  if (!obtenerCorreoDe(usuario)) return;

  await enviarCorreo({
    to: obtenerCorreoDe(usuario),
    subject: '🎉 ¡Terminaste de pagar tu compra!',
    html: `${ENCABEZADO_CORREO(`¡Felicidades, ${usuario.nombre || ''}!`)}<p>Acabas de saldar por completo esta compra. Gracias por confiar en Credynamita.</p>${PIE_CORREO}`,
  });
}

// ═══════════════════════════════════════════════════════════════════
// TAREAS PROGRAMADAS (antes onSchedule, ahora funciones normales que
// llama node-cron más abajo)
// ═══════════════════════════════════════════════════════════════════

async function recordatoriosVencimiento() {
  const hoy = new Date();
  hoy.setHours(0, 0, 0, 0);
  const hoyTexto = hoy.toISOString().split('T')[0];
  const cuotasSnap = await db.collectionGroup('cuotas').where('pagado', '==', false).get();
  let enviados = 0;

  for (const doc of cuotasSnap.docs) {
    const cuota = doc.data();
    if (cuota.estado === 'ANULADA' || !cuota.fechaVencimiento?.toDate) continue;
    const fechaVenc = cuota.fechaVencimiento.toDate();
    fechaVenc.setHours(0, 0, 0, 0);
    const diasRestantes = Math.round((fechaVenc - hoy) / (1000 * 60 * 60 * 24));
    if (![7, 3, 1].includes(diasRestantes)) continue;
    if (cuota.ultimoRecordatorioDias === diasRestantes && cuota.ultimoRecordatorioFecha === hoyTexto) continue;

    const uid = doc.ref.parent.parent?.id;
    if (!uid) continue;
    const usuarioSnap = await db.collection('usuarios').doc(uid).get();
    const usuario = usuarioSnap.data();
    const correoDestino = obtenerCorreoDe(usuario);
    const pushToken = usuario?.expoPushToken;
    if (!correoDestino && !pushToken) continue;

    const mensajeDias = diasRestantes === 1 ? 'mañana' : `en ${diasRestantes} días`;
    const montoAMostrar = Number(cuota.saldoPendiente ?? cuota.monto ?? 0);

    if (correoDestino) {
      await enviarCorreo({
        to: correoDestino,
        subject: `Recordatorio: tu cuota vence ${mensajeDias}`,
        html: `${ENCABEZADO_CORREO(`Hola ${usuario.nombre || ''}`)}<p>Tu cuota <strong>#${cuota.numeroCuota}</strong> de <strong>$${montoAMostrar.toFixed(2)}</strong> vence ${mensajeDias} (${fechaVenc.toLocaleDateString('es-ES')}).</p><p>Realiza tu pago a tiempo para mantener tu crédito al día.</p>${PIE_CORREO}`,
      });
    }
    if (pushToken) {
      await enviarPush({
        to: pushToken,
        title: diasRestantes === 1 ? '¡Tu cuota vence mañana!' : `Tu cuota vence en ${diasRestantes} días`,
        body: `Cuota #${cuota.numeroCuota} — $${montoAMostrar.toFixed(2)}, vence el ${fechaVenc.toLocaleDateString('es-ES')}.`,
      });
    }
    await doc.ref.update({ ultimoRecordatorioDias: diasRestantes, ultimoRecordatorioFecha: hoyTexto });
    enviados++;
  }
  logger.log(`recordatoriosVencimiento: ${enviados} recordatorio(s) enviado(s).`);
}

async function suspenderPorMora() {
  const hoy = new Date();
  hoy.setHours(0, 0, 0, 0);
  const cuotasSnap = await db.collectionGroup('cuotas').where('pagado', '==', false).get();
  const uidsConMora = new Set();

  for (const doc of cuotasSnap.docs) {
    const cuota = doc.data();
    if (cuota.estado === 'ANULADA' || !cuota.fechaVencimiento?.toDate) continue;
    const fechaVenc = cuota.fechaVencimiento.toDate();
    fechaVenc.setHours(0, 0, 0, 0);
    const diasVencida = Math.round((hoy - fechaVenc) / (1000 * 60 * 60 * 24));
    if (diasVencida >= 3) {
      const uid = doc.ref.parent.parent?.id;
      if (uid) uidsConMora.add(uid);
    }
  }

  let suspendidos = 0;
  for (const uid of uidsConMora) {
    const usuarioRef = db.collection('usuarios').doc(uid);
    const usuarioSnap = await usuarioRef.get();
    const usuario = usuarioSnap.data();
    if (!usuario || usuario.creditoSuspendido === true) continue;

    await usuarioRef.update({ creditoSuspendido: true, fechaSuspension: FieldValue.serverTimestamp() });
    suspendidos++;

    const correoDestino = obtenerCorreoDe(usuario);
    if (correoDestino) {
      await enviarCorreo({
        to: correoDestino,
        subject: '⚠️ Tu crédito fue suspendido temporalmente',
        html: `${ENCABEZADO_CORREO(`Hola ${usuario.nombre || ''}`)}<p>Detectamos que tienes al menos una cuota vencida hace <strong>3 días o más</strong>, así que tu crédito disponible quedó <strong>suspendido</strong> — no podrás hacer compras nuevas hasta ponerte al día.</p><p>En cuanto pagues la(s) cuota(s) vencida(s), tu crédito se reactiva automáticamente, sin que tengas que hacer nada más.</p>${PIE_CORREO}`,
      });
    }
    if (usuario.expoPushToken) {
      await enviarPush({ to: usuario.expoPushToken, title: 'Crédito suspendido', body: 'Tienes una cuota vencida hace 3 días o más. Ponte al día para reactivar tu crédito.' });
    }
  }
  logger.log(`suspenderPorMora: ${suspendidos} cuenta(s) suspendida(s) nueva(s).`);
}

async function avisoDiarioCuotaVencida() {
  const hoy = new Date();
  hoy.setHours(0, 0, 0, 0);
  const cuotasSnap = await db.collectionGroup('cuotas').where('pagado', '==', false).get();
  let enviados = 0;

  for (const doc of cuotasSnap.docs) {
    const cuota = doc.data();
    if (cuota.estado === 'ANULADA' || !cuota.fechaVencimiento?.toDate) continue;
    const fechaVenc = cuota.fechaVencimiento.toDate();
    fechaVenc.setHours(0, 0, 0, 0);
    const diasVencida = Math.round((hoy - fechaVenc) / (1000 * 60 * 60 * 24));
    if (diasVencida < 1) continue;

    const uid = doc.ref.parent.parent?.id;
    if (!uid) continue;
    const usuarioSnap = await db.collection('usuarios').doc(uid).get();
    const usuario = usuarioSnap.data();
    const correoDestino = obtenerCorreoDe(usuario);
    const pushToken = usuario?.expoPushToken;
    if (!correoDestino && !pushToken) continue;

    const montoAMostrar = Number(cuota.saldoPendiente ?? cuota.monto ?? 0);
    const textoDias = `${diasVencida} día${diasVencida === 1 ? '' : 's'}`;

    if (correoDestino) {
      await enviarCorreo({
        to: correoDestino,
        subject: `⚠️ Tienes una cuota vencida hace ${textoDias}`,
        html: `${ENCABEZADO_CORREO(`Hola ${usuario.nombre || ''}`)}<p>Tu cuota <strong>#${cuota.numeroCuota}</strong> de <strong>$${montoAMostrar.toFixed(2)}</strong> venció el ${fechaVenc.toLocaleDateString('es-ES')} — hace ${textoDias}.</p><p>Realiza tu pago cuanto antes para evitar la suspensión de tu crédito disponible.</p>${PIE_CORREO}`,
      });
    }
    if (pushToken) {
      await enviarPush({ to: pushToken, title: 'Tienes una cuota vencida', body: `Cuota #${cuota.numeroCuota} — $${montoAMostrar.toFixed(2)}, vencida hace ${textoDias}.` });
    }
    enviados++;
  }
  logger.log(`avisoDiarioCuotaVencida: ${enviados} aviso(s) enviado(s).`);
}

// ═══════════════════════════════════════════════════════════════════
// LISTENERS DE FIRESTORE (reemplazan los triggers onDocumentCreated /
// onDocumentUpdated)
//
// Esto solo es posible porque Render corre este servidor como un
// proceso QUE NUNCA SE APAGA — puede quedarse "escuchando" cambios en
// tiempo real, como haría la propia app. Una Cloud Function no podía
// hacer esto (se apaga entre invocaciones), por eso ahí se usaban
// triggers separados en vez de un listener persistente.
//
// OJO con un detalle importante: cuando un listener se conecta por
// primera vez, Firestore le manda TODOS los documentos existentes como
// si fueran "nuevos" — hay que ignorar esa primera tanda, o si no,
// cada vez que el servidor arranque (o se reinicie) se reenviarían
// TODOS los correos de bienvenida, compras, etc. de nuevo. Por eso cada
// listener de abajo tiene una bandera "primeraCarga".
// ═══════════════════════════════════════════════════════════════════

function iniciarListeners() {
  // 1. BIENVENIDA — nuevo documento en /usuarios/{uid}
  let primeraCargaUsuarios = true;
  const limitesConocidos = new Map(); // uid -> limite_total (para detectar cambios)

  db.collection('usuarios').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const usuario = change.doc.data();
      const uid = change.doc.id;

      if (change.type === 'added') {
        limitesConocidos.set(uid, usuario.limite_total);
        if (!primeraCargaUsuarios) {
          correoBienvenida(uid, usuario).catch((e) => logger.error('Error en correoBienvenida:', e));
        }
      } else if (change.type === 'modified') {
        const limiteAnterior = limitesConocidos.get(uid);
        limitesConocidos.set(uid, usuario.limite_total);
        if (!primeraCargaUsuarios && limiteAnterior !== undefined) {
          correoCambioLimite(usuario, limiteAnterior).catch((e) => logger.error('Error en correoCambioLimite:', e));
        }
      }
    });
    primeraCargaUsuarios = false;
  }, (error) => logger.error('Error en listener de usuarios:', error));

  // 2. COMPRA REGISTRADA — nuevo documento en /usuarios/{uid}/creditos/{id}
  let primeraCargaCreditos = true;
  db.collectionGroup('creditos').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      if (change.type !== 'added') return;
      if (primeraCargaCreditos) return;
      const uid = change.doc.ref.parent.parent?.id;
      if (!uid) return;
      correoNuevaCompra(uid, change.doc.id, change.doc.data())
        .catch((e) => logger.error('Error en correoNuevaCompra:', e));
    });
    primeraCargaCreditos = false;
  }, (error) => logger.error('Error en listener de creditos:', error));

  // 3 y 4. PAGO APROBADO/RECHAZADO — cambio de estado en /pagos/{id}
  let primeraCargaPagos = true;
  const estadosConocidosPagos = new Map(); // pagoId -> status

  db.collection('pagos').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const pago = change.doc.data();
      const pagoId = change.doc.id;

      if (change.type === 'added') {
        estadosConocidosPagos.set(pagoId, pago.status);
      } else if (change.type === 'modified') {
        const estadoAnterior = estadosConocidosPagos.get(pagoId);
        estadosConocidosPagos.set(pagoId, pago.status);
        const cambioDeVerdad = estadoAnterior !== undefined && estadoAnterior !== pago.status;
        const esRelevante = pago.status === 'aprobado' || pago.status === 'rechazado';
        if (!primeraCargaPagos && cambioDeVerdad && esRelevante) {
          correoEstadoPago(pago).catch((e) => logger.error('Error en correoEstadoPago:', e));
        }
      }
    });
    primeraCargaPagos = false;
  }, (error) => logger.error('Error en listener de pagos:', error));

  // 5. COMPRA SALDADA — una cuota pasa a pagado:true en
  // /usuarios/{uid}/cuotas/{id}
  let primeraCargaCuotas = true;
  const pagadoConocido = new Map(); // ruta completa del doc -> pagado (true/false)

  db.collectionGroup('cuotas').onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      const cuota = change.doc.data();
      const ruta = change.doc.ref.path;

      if (change.type === 'added') {
        pagadoConocido.set(ruta, !!cuota.pagado);
      } else if (change.type === 'modified') {
        const pagadoAntes = pagadoConocido.get(ruta);
        pagadoConocido.set(ruta, !!cuota.pagado);
        const pasoAPagada = !primeraCargaCuotas && pagadoAntes === false && cuota.pagado === true;
        if (pasoAPagada) {
          const uid = change.doc.ref.parent.parent?.id;
          if (uid && cuota.creditoId) {
            correoCompraSaldada(uid, cuota.creditoId).catch((e) => logger.error('Error en correoCompraSaldada:', e));
          }
        }
      }
    });
    primeraCargaCuotas = false;
  }, (error) => logger.error('Error en listener de cuotas:', error));

  logger.log('Listeners de Firestore conectados (bienvenida, cambio de límite, nueva compra, estado de pago, compra saldada).');
}

// ═══════════════════════════════════════════════════════════════════
// TAREAS PROGRAMADAS CON node-cron
//
// Los horarios se pusieron en hora de Venezuela (America/Caracas,
// UTC-4) explícitamente — en Cloud Functions no se había fijado zona
// horaria, así que corrían en UTC por defecto (8:00 UTC = 4:00 am en
// Venezuela). Si notas que los avisos llegaban de madrugada antes,
// era por eso — ahora si van a llegar a las 8 am hora de Venezuela de
// verdad. Si prefieres mantener el horario viejo (UTC), avísame y lo
// ajustamos.
// ═══════════════════════════════════════════════════════════════════

const ZONA_HORARIA = 'America/Caracas';

cron.schedule('*/30 * * * *', () => {
  revisarPagosNoEnviados().catch((e) => logger.error('Error en revisarPagosNoEnviados:', e));
}, { timezone: ZONA_HORARIA });

cron.schedule('0 8 * * *', () => {
  recordatoriosVencimiento().catch((e) => logger.error('Error en recordatoriosVencimiento:', e));
}, { timezone: ZONA_HORARIA });

cron.schedule('10 8 * * *', () => {
  suspenderPorMora().catch((e) => logger.error('Error en suspenderPorMora:', e));
}, { timezone: ZONA_HORARIA });

cron.schedule('15 8 * * *', () => {
  avisoDiarioCuotaVencida().catch((e) => logger.error('Error en avisoDiarioCuotaVencida:', e));
}, { timezone: ZONA_HORARIA });

// ═══════════════════════════════════════════════════════════════════
// ARRANQUE DEL SERVIDOR
// ═══════════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.log(`Servidor Credynamita escuchando en el puerto ${PORT}.`);
  iniciarListeners();
});
